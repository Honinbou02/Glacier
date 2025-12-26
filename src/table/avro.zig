// ═══════════════════════════════════════════════════════════════════════════
// APACHE AVRO BINARY FORMAT PARSER
// ═══════════════════════════════════════════════════════════════════════════
//
// Avro is used by Iceberg to store manifest files.
//
// BINARY ENCODING:
//   - Integers: Zigzag + Varint encoding
//   - Longs: Zigzag + Varint encoding
//   - Strings: Length prefix (long) + UTF-8 bytes
//   - Bytes: Length prefix (long) + raw bytes
//   - Boolean: Single byte (0 or 1)
//   - Arrays: Series of blocks (count + items), terminated by 0
//   - Records: Concatenated field values (no delimiters)
//
// ICEBERG MANIFEST SCHEMA:
//   - status: int (0=EXISTING, 1=ADDED, 2=DELETED)
//   - snapshot_id: long
//   - data_file: record {
//       file_path: string
//       file_format: string
//       partition: map<string, string>
//       record_count: long
//       file_size_in_bytes: long
//       column_sizes: map<int, long>
//       value_counts: map<int, long>
//       null_value_counts: map<int, long>
//       lower_bounds: map<int, bytes>
//       upper_bounds: map<int, bytes>
//     }
//
// REFERENCES:
//   - https://avro.apache.org/docs/current/spec.html
//   - https://iceberg.apache.org/spec/#manifests
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════
// VARINT ENCODING (Zigzag + Variable Length)
// ═══════════════════════════════════════════════════════════════════════════

/// Read a variable-length integer (used for both int and long)
/// Returns (value, bytes_read)
pub fn readVarint(data: []const u8) !struct { value: i64, bytes_read: usize } {
    var result: u64 = 0;
    var shift: u6 = 0;
    var pos: usize = 0;

    while (pos < data.len) : (pos += 1) {
        const byte = data[pos];
        const value = @as(u64, byte & 0x7F);

        result |= value << shift;

        // If MSB is 0, we're done
        if ((byte & 0x80) == 0) {
            // Zigzag decode: (n >>> 1) ^ -(n & 1)
            const unsigned = result;
            const signed = @as(i64, @bitCast((unsigned >> 1) ^ (~(unsigned & 1) +% 1)));
            return .{ .value = signed, .bytes_read = pos + 1 };
        }

        shift += 7;
        if (shift >= 64) {
            return error.VarintTooLarge;
        }
    }

    return error.UnexpectedEndOfData;
}

/// Read variable-length unsigned integer (for array/map counts)
pub fn readUnsignedVarint(data: []const u8) !struct { value: u64, bytes_read: usize } {
    var result: u64 = 0;
    var shift: u6 = 0;
    var pos: usize = 0;

    while (pos < data.len) : (pos += 1) {
        const byte = data[pos];
        const value = @as(u64, byte & 0x7F);

        result |= value << shift;

        if ((byte & 0x80) == 0) {
            return .{ .value = result, .bytes_read = pos + 1 };
        }

        shift += 7;
        if (shift >= 64) {
            return error.VarintTooLarge;
        }
    }

    return error.UnexpectedEndOfData;
}

// ═══════════════════════════════════════════════════════════════════════════
// AVRO BINARY DECODER
// ═══════════════════════════════════════════════════════════════════════════

pub const AvroBinaryDecoder = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    /// Read boolean (1 byte: 0 or 1)
    pub fn readBoolean(self: *Self) !bool {
        if (self.pos >= self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const value = self.data[self.pos];
        self.pos += 1;

        return value != 0;
    }

    /// Read int (zigzag varint)
    pub fn readInt(self: *Self) !i32 {
        const result = try readVarint(self.data[self.pos..]);
        self.pos += result.bytes_read;

        if (result.value < std.math.minInt(i32) or result.value > std.math.maxInt(i32)) {
            return error.IntegerOverflow;
        }

        return @as(i32, @intCast(result.value));
    }

    /// Read long (zigzag varint)
    pub fn readLong(self: *Self) !i64 {
        const result = try readVarint(self.data[self.pos..]);
        self.pos += result.bytes_read;

        return result.value;
    }

    /// Read string (length-prefixed UTF-8)
    /// Returns a slice pointing into the original data (zero-copy)
    pub fn readString(self: *Self) ![]const u8 {
        const length_result = try readVarint(self.data[self.pos..]);
        self.pos += length_result.bytes_read;

        if (length_result.value < 0) {
            return error.NegativeLength;
        }

        const length = @as(usize, @intCast(length_result.value));

        if (self.pos + length > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const string = self.data[self.pos .. self.pos + length];
        self.pos += length;

        return string;
    }

    /// Read bytes (length-prefixed)
    /// Returns a slice pointing into the original data (zero-copy)
    pub fn readBytes(self: *Self) ![]const u8 {
        // Same as readString for binary encoding
        return try self.readString();
    }

    /// Skip a value of unknown type (advance position without decoding)
    pub fn skip(self: *Self, bytes: usize) !void {
        if (self.pos + bytes > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        self.pos += bytes;
    }

    /// Get current position
    pub fn getPosition(self: *const Self) usize {
        return self.pos;
    }

    /// Check if at end of data
    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// ICEBERG MANIFEST ENTRY
// ═══════════════════════════════════════════════════════════════════════════

/// Map of field_id → raw bytes (Iceberg bounds format)
/// Used for lower_bounds and upper_bounds in Iceberg manifest files
pub const BoundsMap = std.AutoHashMap(i32, []const u8);

/// Status of a data file in the manifest
pub const FileStatus = enum(i32) {
    existing = 0,
    added = 1,
    deleted = 2,
};

/// Data file entry from Iceberg manifest
pub const DataFileEntry = struct {
    status: FileStatus,
    snapshot_id: i64,
    file_path: []const u8,
    file_format: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,

    // CORREÇÃO AQUI: Adicionado ' = null' para permitir inicialização sem esses campos
    lower_bounds: ?BoundsMap = null, // Iceberg bounds for predicate pushdown (field_id → bytes)
    upper_bounds: ?BoundsMap = null, // Iceberg bounds for predicate pushdown (field_id → bytes)

    allocator: std.mem.Allocator,

    pub fn deinit(self: *DataFileEntry) void {
        self.allocator.free(self.file_path);
        self.allocator.free(self.file_format);

        // Free bounds maps
        if (self.lower_bounds) |*bounds| {
            var it = bounds.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.value_ptr.*);
            }
            bounds.deinit();
        }

        if (self.upper_bounds) |*bounds| {
            var it = bounds.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.value_ptr.*);
            }
            bounds.deinit();
        }
    }
};

/// Parse a single manifest entry
/// This is a simplified parser that reads only the essential fields
pub fn parseManifestEntry(allocator: std.mem.Allocator, decoder: *AvroBinaryDecoder) !DataFileEntry {
    const start_pos = decoder.pos;
    std.debug.print("  [ENTRY] Starting parse at pos {d}, {d} bytes remaining\n", .{ start_pos, decoder.data.len - start_pos });

    // Read status (int)
    const status_int = try decoder.readInt();
    const status: FileStatus = @enumFromInt(status_int);
    std.debug.print("  [ENTRY] Read status: {d}\n", .{status_int});

    // Read snapshot_id (union: null or long)
    // In Iceberg manifest, snapshot_id is ["null", "long"]
    const snapshot_id_union_idx = try decoder.readLong();
    std.debug.print("  [ENTRY] Read snapshot_id union index: {d}\n", .{snapshot_id_union_idx});

    var snapshot_id: i64 = 0;
    if (snapshot_id_union_idx == 1) {
        // Index 1 = long (has value)
        snapshot_id = try decoder.readLong();
        std.debug.print("  [ENTRY] Read snapshot_id value: {d}\n", .{snapshot_id});
    } else {
        // Index 0 = null
        std.debug.print("  [ENTRY] snapshot_id is null\n", .{});
    }

    // Read data_file record
    // Field 1: file_path (string)
    const file_path_slice = try decoder.readString();
    std.debug.print("  [ENTRY] Read file_path: {s} (len {d})\n", .{ file_path_slice, file_path_slice.len });
    const file_path = try allocator.dupe(u8, file_path_slice);
    errdefer allocator.free(file_path);

    // Field 2: file_format (string)
    const file_format_slice = try decoder.readString();
    std.debug.print("  [ENTRY] Read file_format: {s}\n", .{file_format_slice});
    const file_format = try allocator.dupe(u8, file_format_slice);
    errdefer allocator.free(file_format);

    // Field 3: partition (map<string, string>) - skip for now
    // Read map as array of key-value pairs
    const partition_count_result = try readVarint(decoder.data[decoder.pos..]);
    decoder.pos += partition_count_result.bytes_read;
    var remaining = partition_count_result.value;

    while (remaining != 0) {
        if (remaining < 0) {
            return error.InvalidMapCount;
        }

        // Skip key-value pairs
        var i: i64 = 0;
        while (i < remaining) : (i += 1) {
            _ = try decoder.readString(); // key
            _ = try decoder.readString(); // value
        }

        // Read next block count
        const next_count = try readVarint(decoder.data[decoder.pos..]);
        decoder.pos += next_count.bytes_read;
        remaining = next_count.value;
    }

    // Field 4: record_count (long)
    const record_count = try decoder.readLong();
    std.debug.print("  [ENTRY] Read record_count: {d}\n", .{record_count});

    // Field 5: file_size_in_bytes (long)
    const file_size_in_bytes = try decoder.readLong();
    std.debug.print("  [ENTRY] Read file_size: {d}\n", .{file_size_in_bytes});

    // Skip remaining fields (column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds)
    // These are all maps, read until we get 0 count

    // column_sizes: map<int, long>
    try skipMap(decoder);

    // value_counts: map<int, long>
    try skipMap(decoder);

    // null_value_counts: map<int, long>
    try skipMap(decoder);

    // lower_bounds: union [null, map<int, bytes>] - READ for predicate pushdown
    std.debug.print("  [ENTRY] Reading lower_bounds at pos {d}\n", .{decoder.pos});
    const lower_bounds_union = try decoder.readLong();
    const lower_bounds = if (lower_bounds_union == 0)
        null // null branch
    else
        try readBytesMap(allocator, decoder);

    // upper_bounds: union [null, map<int, bytes>] - READ for predicate pushdown
    std.debug.print("  [ENTRY] Reading upper_bounds at pos {d}\n", .{decoder.pos});
    const upper_bounds_union = try decoder.readLong();
    const upper_bounds = if (upper_bounds_union == 0)
        null // null branch
    else
        try readBytesMap(allocator, decoder);

    const end_pos = decoder.pos;
    std.debug.print("  [ENTRY] Finished at pos {d}, consumed {d} bytes\n\n", .{ end_pos, end_pos - start_pos });

    return DataFileEntry{
        .status = status,
        .snapshot_id = snapshot_id,
        .file_path = file_path,
        .file_format = file_format,
        .record_count = record_count,
        .file_size_in_bytes = file_size_in_bytes,
        .lower_bounds = lower_bounds,
        .upper_bounds = upper_bounds,
        .allocator = allocator,
    };
}

/// Read a map<int, bytes> field and return complete map (for bounds)
/// Iceberg bounds are Map<FieldId, ByteBuffer> where keys are stringified field IDs
/// Returns HashMap of field_id → raw bytes for predicate pushdown
fn readBytesMap(allocator: std.mem.Allocator, decoder: *AvroBinaryDecoder) !?BoundsMap {
    const count_result = try readVarint(decoder.data[decoder.pos..]);
    decoder.pos += count_result.bytes_read;
    var remaining = count_result.value;

    // Empty map
    if (remaining == 0) return null;

    var map = BoundsMap.init(allocator);
    errdefer {
        var it = map.iterator();
        while (it.next()) |entry| allocator.free(entry.value_ptr.*);
        map.deinit();
    }

    while (remaining != 0) {
        if (remaining < 0) {
            return error.InvalidMapCount;
        }

        // Read key-value pairs
        var i: i64 = 0;
        while (i < remaining) : (i += 1) {
            // Read key as string, parse to int (field_id)
            const key_str = try decoder.readString();
            const field_id = std.fmt.parseInt(i32, key_str, 10) catch {
                // Skip if not a valid field_id
                _ = try decoder.readBytes();
                continue;
            };

            // Read bytes value
            const bytes_slice = try decoder.readBytes();
            const bytes_copy = try allocator.dupe(u8, bytes_slice);
            errdefer allocator.free(bytes_copy);

            try map.put(field_id, bytes_copy);
        }

        // Read next block count
        const next_count = try readVarint(decoder.data[decoder.pos..]);
        decoder.pos += next_count.bytes_read;
        remaining = next_count.value;
    }

    return map;
}

/// Skip a map field
fn skipMap(decoder: *AvroBinaryDecoder) !void {
    const count_result = try readVarint(decoder.data[decoder.pos..]);
    decoder.pos += count_result.bytes_read;
    var remaining = count_result.value;

    while (remaining != 0) {
        if (remaining < 0) {
            return error.InvalidMapCount;
        }

        // Skip key-value pairs
        var i: i64 = 0;
        while (i < remaining) : (i += 1) {
            _ = try decoder.readInt(); // key (int)
            _ = try decoder.readLong(); // value (long or bytes - read as long for now)
        }

        // Read next block count
        const next_count = try readVarint(decoder.data[decoder.pos..]);
        decoder.pos += next_count.bytes_read;
        remaining = next_count.value;
    }
}

/// Read JSON manifest list (Iceberg v2 format)
fn readJsonManifestList(allocator: std.mem.Allocator, json_data: []const u8) ![]DataFileEntry {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_data, .{});
    defer parsed.deinit();

    const root = parsed.value;
    if (root != .array) {
        return error.InvalidJsonManifest;
    }

    const manifest_entries = root.array.items;
    var entries: std.ArrayListUnmanaged(DataFileEntry) = .{};
    errdefer {
        for (entries.items) |*entry| {
            var mut_entry = entry.*;
            mut_entry.deinit();
        }
        entries.deinit(allocator);
    }

    // JSON manifest list has a different structure - it points directly to data files
    for (manifest_entries) |manifest_entry| {
        if (manifest_entry != .object) continue;
        const obj = manifest_entry.object;

        // Get manifest_path (this is the actual data file path in JSON format)
        const manifest_path = if (obj.get("manifest_path")) |mp|
            try allocator.dupe(u8, mp.string)
        else
            continue;
        errdefer allocator.free(manifest_path);

        // Get record count
        const record_count = if (obj.get("added_rows_count")) |rc|
            @as(i64, @intCast(rc.integer))
        else
            0;

        // Get snapshot_id
        const snapshot_id = if (obj.get("added_snapshot_id")) |sid|
            @as(i64, @intCast(sid.integer))
        else
            0;

        // Create a DataFileEntry
        // In JSON format, manifest_path IS the data file path
        const file_format = try allocator.dupe(u8, "PARQUET");
        errdefer allocator.free(file_format);

        const entry = DataFileEntry{
            .status = .added, // JSON manifests list added files
            .snapshot_id = snapshot_id,
            .file_path = manifest_path,
            .file_format = file_format,
            .record_count = record_count,
            .file_size_in_bytes = if (obj.get("manifest_length")) |ml| @as(i64, @intCast(ml.integer)) else 0,
            .lower_bounds = null,
            .upper_bounds = null,
            .allocator = allocator,
        };

        try entries.append(allocator, entry);
    }

    return entries.toOwnedSlice(allocator);
}

/// Read manifest file from disk and parse all entries
pub fn readManifestFile(allocator: std.mem.Allocator, file_path: []const u8) ![]DataFileEntry {
    // Open file
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    // Read entire file
    const file_size = (try file.stat()).size;
    const file_data = try allocator.alloc(u8, file_size);
    defer allocator.free(file_data);

    _ = try file.read(file_data);

    // Detect format: Avro (binary) or JSON
    // Avro starts with "Obj" + version byte
    // JSON starts with '[' or whitespace then '['

    // Skip leading whitespace to check for JSON
    var start_idx: usize = 0;
    while (start_idx < file_data.len and std.ascii.isWhitespace(file_data[start_idx])) : (start_idx += 1) {}

    // Check if it's JSON manifest list
    if (start_idx < file_data.len and file_data[start_idx] == '[') {
        std.debug.print("DEBUG: Detected JSON manifest list format\n", .{});
        return try readJsonManifestList(allocator, file_data);
    }

    // Check if it's Avro format
    if (file_data.len < 4 or !std.mem.eql(u8, file_data[0..3], "Obj")) {
        std.debug.print("ERROR: Invalid manifest file format (not Avro or JSON)\n", .{});
        return error.InvalidManifestFile;
    }

    std.debug.print("DEBUG: Avro file version {d}, size {d} bytes\n", .{ file_data[3], file_data.len });

    // Skip Avro header (magic + metadata map + sync marker)
    // After magic (4 bytes), there's a metadata map (key-value pairs)
    // Followed by 16-byte sync marker
    // Then data blocks begin

    var pos: usize = 4; // After magic

    // Read metadata map (simplified - just skip it)
    // Map format: count, key-value pairs
    const map_count_result = try readVarint(file_data[pos..]);
    pos += map_count_result.bytes_read;
    std.debug.print("DEBUG: Metadata map has {d} entries\n", .{map_count_result.value});

    // Skip all metadata entries
    var entries_remaining = map_count_result.value;
    while (entries_remaining > 0) : (entries_remaining -= 1) {
        // Read key (string)
        const key_len_result = try readVarint(file_data[pos..]);
        pos += key_len_result.bytes_read;
        const key_len: usize = @intCast(if (key_len_result.value < 0) -key_len_result.value else key_len_result.value);
        pos += key_len; // Skip key bytes

        // Read value (bytes)
        const val_len_result = try readVarint(file_data[pos..]);
        pos += val_len_result.bytes_read;
        const val_len: usize = @intCast(if (val_len_result.value < 0) -val_len_result.value else val_len_result.value);
        pos += val_len; // Skip value bytes
    }

    // Read map terminator (should be 0)
    const map_end = try readVarint(file_data[pos..]);
    pos += map_end.bytes_read;

    // Read 16-byte sync marker
    if (pos + 16 > file_data.len) {
        return error.InvalidAvroFile;
    }
    const sync_marker = file_data[pos .. pos + 16];
    pos += 16;

    std.debug.print("DEBUG: Header parsed, data starts at offset {d}\n", .{pos});
    std.debug.print("DEBUG: Sync marker: ", .{});
    for (sync_marker) |b| std.debug.print("{x:0>2}", .{b});
    std.debug.print("\n", .{});

    const data_start = pos;
    if (data_start >= file_data.len) {
        return error.InvalidAvroFile;
    }

    var entries: std.ArrayListUnmanaged(DataFileEntry) = .{};
    errdefer {
        for (entries.items) |*entry| {
            var mut_entry = entry.*;
            mut_entry.deinit();
        }
        entries.deinit(allocator);
    }

    // Read Avro data blocks
    // Each block: object_count (long), byte_count (long), objects..., sync_marker (16 bytes)
    pos = data_start;
    var blocks_read: usize = 0;

    while (pos < file_data.len) {
        // Read object count
        const count_result = try readVarint(file_data[pos..]);
        pos += count_result.bytes_read;

        if (count_result.value == 0) break; // End of file
        if (count_result.value < 0) return error.InvalidAvroFile;

        // Read byte count
        const size_result = try readVarint(file_data[pos..]);
        pos += size_result.bytes_read;

        if (size_result.value < 0) return error.InvalidAvroFile;
        const block_size: usize = @intCast(size_result.value);

        std.debug.print("DEBUG: Block {d}: {d} objects, {d} bytes\n", .{ blocks_read, count_result.value, block_size });

        // Read objects from this block
        var decoder = AvroBinaryDecoder.init(file_data[pos .. pos + block_size]);
        var i: i64 = 0;
        while (i < count_result.value) : (i += 1) {
            const entry = parseManifestEntry(allocator, &decoder) catch |err| {
                std.debug.print("Warning: Failed to parse entry {d}/{d}: {}\n", .{ i + 1, count_result.value, err });
                break;
            };
            try entries.append(allocator, entry);
        }

        pos += block_size;

        // Skip sync marker (16 bytes)
        if (pos + 16 > file_data.len) break;
        pos += 16;

        blocks_read += 1;
        if (blocks_read >= 10) break; // Limit for testing
    }

    std.debug.print("DEBUG: Total blocks read: {d}, total entries: {d}\n", .{ blocks_read, entries.items.len });
    return try entries.toOwnedSlice(allocator);
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "readVarint - positive numbers" {
    // 0 = 0x00
    {
        const data = [_]u8{0x00};
        const result = try readVarint(&data);
        try std.testing.expectEqual(@as(i64, 0), result.value);
        try std.testing.expectEqual(@as(usize, 1), result.bytes_read);
    }

    // 1 = zigzag(1) = 2 = 0x02
    {
        const data = [_]u8{0x02};
        const result = try readVarint(&data);
        try std.testing.expectEqual(@as(i64, 1), result.value);
        try std.testing.expectEqual(@as(usize, 1), result.bytes_read);
    }

    // -1 = zigzag(-1) = 1 = 0x01
    {
        const data = [_]u8{0x01};
        const result = try readVarint(&data);
        try std.testing.expectEqual(@as(i64, -1), result.value);
        try std.testing.expectEqual(@as(usize, 1), result.bytes_read);
    }

    // 64 = zigzag(64) = 128 = 0x80 0x01
    {
        const data = [_]u8{ 0x80, 0x01 };
        const result = try readVarint(&data);
        try std.testing.expectEqual(@as(i64, 64), result.value);
        try std.testing.expectEqual(@as(usize, 2), result.bytes_read);
    }
}

test "AvroBinaryDecoder - primitives" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Boolean
    {
        const data = [_]u8{ 0x01, 0x00 };
        var decoder = AvroBinaryDecoder.init(&data);
        try std.testing.expectEqual(true, try decoder.readBoolean());
        try std.testing.expectEqual(false, try decoder.readBoolean());
    }

    // Int
    {
        const data = [_]u8{
            0x00, // 0
            0x02, // 1
            0x01, // -1
        };
        var decoder = AvroBinaryDecoder.init(&data);
        try std.testing.expectEqual(@as(i32, 0), try decoder.readInt());
        try std.testing.expectEqual(@as(i32, 1), try decoder.readInt());
        try std.testing.expectEqual(@as(i32, -1), try decoder.readInt());
    }

    // Long
    {
        const data = [_]u8{
            0x00, // 0
            0x02, // 1
        };
        var decoder = AvroBinaryDecoder.init(&data);
        try std.testing.expectEqual(@as(i64, 0), try decoder.readLong());
        try std.testing.expectEqual(@as(i64, 1), try decoder.readLong());
    }

    // String
    {
        // "hello" = length 5 (zigzag = 10 = 0x0A) + "hello"
        const data = [_]u8{ 0x0A, 'h', 'e', 'l', 'l', 'o' };
        var decoder = AvroBinaryDecoder.init(&data);
        const str = try decoder.readString();
        try std.testing.expectEqualStrings("hello", str);
    }
}

test "AvroBinaryDecoder - string encoding" {
    // Test the actual varint encoding for string length
    // Length 5 should be encoded as zigzag(5) = 10 = 0x0A
    const data = [_]u8{ 0x0A, 'h', 'e', 'l', 'l', 'o' };
    var decoder = AvroBinaryDecoder.init(&data);
    const str = try decoder.readString();
    try std.testing.expectEqualStrings("hello", str);
}
