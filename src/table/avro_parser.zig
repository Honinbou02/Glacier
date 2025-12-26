const std = @import("std");
const reader_mod = @import("avro_reader.zig");
const types = @import("avro_types.zig");
const file_mapper = @import("file_mapper.zig");

pub const ManifestData = struct {
    schema_id: u32,
    partition_spec: []const u8,
    file_count: usize,
    total_size: u64,

    pub fn deinit(self: ManifestData, allocator: std.mem.Allocator) void {
        allocator.free(self.partition_spec);
    }
};

pub const FileList = struct {
    files: []FileInfo,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *FileList) void {
        for (self.files) |*file| {
            self.allocator.free(file.path);
            if (file.partition) |p| self.allocator.free(p);
        }
        self.allocator.free(self.files);
    }
};

pub const FileInfo = struct {
    path: []const u8,
    size: u64,
    record_count: i64,
    partition: ?[]const u8,
};

pub const ManifestEntry = struct {
    status: i32,
    snapshot_id: ?i64,
    sequence_number: ?i64,
    file_sequence_number: ?i64,
    data_file: DataFile,
};

pub const DataFile = struct {
    content: i32,
    file_path: []const u8,
    file_format: []const u8,
    partition: ?[]const u8,
    record_count: i64,
    file_size_in_bytes: i64,
    block_size_in_bytes: ?i64,
    column_sizes: ?[]const u8,
    value_counts: ?[]const u8,
    null_value_counts: ?[]const u8,
    nan_value_counts: ?[]const u8,
    lower_bounds: ?[]const u8,
    upper_bounds: ?[]const u8,
    key_metadata: ?[]const u8,
    split_offsets: ?[]const u8,
    equality_ids: ?[]const u8,
    sort_order_id: ?i32,
};

pub fn parseManifest(allocator: std.mem.Allocator, path: []const u8) !ManifestData {
    // Use memory mapping for zero-copy file access
    // This reduces RAM usage dramatically (OS loads pages on demand)
    var mapped = try file_mapper.MappedFile.init(path);
    defer mapped.deinit();

    // Parse Avro container with memory-mapped data
    var avro_reader = reader_mod.AvroReader.init(allocator, mapped.data);

    var header = try avro_reader.readHeader();
    defer header.deinit();

    // Extract schema from metadata
    const schema_bytes = header.metadata.get("avro.schema") orelse return error.MissingSchema;
    _ = schema_bytes;

    // Parse data blocks and count entries
    var file_count: usize = 0;
    var total_size: u64 = 0;

    while (try avro_reader.readDataBlock(header.sync_marker)) |block| {
        // Parse entries in this block
        var block_reader = reader_mod.AvroReader.init(allocator, block.data);

        var i: i64 = 0;
        while (i < block.count) : (i += 1) {
            const entry = try parseManifestEntry(&block_reader);
            file_count += 1;
            total_size += @as(u64, @intCast(entry.data_file.file_size_in_bytes));
        }
    }

    return ManifestData{
        .schema_id = 0,
        .partition_spec = try allocator.dupe(u8, "unknown"),
        .file_count = file_count,
        .total_size = total_size,
    };
}

fn parseManifestEntry(r: *reader_mod.AvroReader) !ManifestEntry {
    // Parse manifest entry record
    // Field order in Iceberg manifest schema:
    // 1. status (int/enum: 0=EXISTING, 1=ADDED, 2=DELETED)
    // 2. snapshot_id (union: null, long)
    // 3. sequence_number (union: null, long)
    // 4. file_sequence_number (union: null, long)
    // 5. data_file (record)

    const status = try r.readInt();

    // Read union index for snapshot_id
    const snapshot_id_union_idx = try r.readLong();
    const snapshot_id: ?i64 = if (snapshot_id_union_idx == 0) null else try r.readLong();

    // Read union index for sequence_number
    const seq_num_union_idx = try r.readLong();
    const sequence_number: ?i64 = if (seq_num_union_idx == 0) null else try r.readLong();

    // Read union index for file_sequence_number
    const file_seq_num_union_idx = try r.readLong();
    const file_sequence_number: ?i64 = if (file_seq_num_union_idx == 0) null else try r.readLong();

    const data_file = try parseDataFile(r);

    return ManifestEntry{
        .status = status,
        .snapshot_id = snapshot_id,
        .sequence_number = sequence_number,
        .file_sequence_number = file_sequence_number,
        .data_file = data_file,
    };
}

fn parseDataFile(r: *reader_mod.AvroReader) !DataFile {
    // Iceberg V2 data_file parser - Reads required fields (1-6) and skips optional fields (7-17)
    // This ensures compatibility with both simplified test manifests and full production manifests.
    //
    // CRITICAL: Avro is sequential - if we don't consume all fields in a record, the read pointer
    // (self.pos) will be misaligned, causing the next record to read garbage data or crash.
    //
    // Full Iceberg V2 data_file schema (17 fields):
    // Fields 1-6: Required (content, file_path, file_format, partition, record_count, file_size_in_bytes)
    // Fields 7-17: Optional statistics (block_size, column_sizes, value_counts, etc.)

    // ===== REQUIRED FIELDS (1-6) =====

    // Field 1: content (int) - 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
    const content = try r.readInt();

    // Field 2: file_path (string)
    const file_path = try r.readString();

    // Field 3: file_format (string) - "PARQUET", "ORC", "AVRO"
    const file_format = try r.readString();

    // Field 4: partition (union: null, struct)
    const partition_union_idx = try r.readLong();
    const partition: ?[]const u8 = if (partition_union_idx == 0)
        null
    else
        try r.readBytes();

    // Field 5: record_count (long)
    const record_count = try r.readLong();

    // Field 6: file_size_in_bytes (long)
    const file_size_in_bytes = try r.readLong();

    // ===== OPTIONAL FIELDS (7-17) - Skip to align read pointer =====
    // IMPORTANT: Iceberg V2 manifests have 17 fields total. Test manifests may have only 6.
    //
    // Check if there are more bytes to read in this record. If yes, we have a V2 manifest
    // with extra fields that need to be consumed to keep the read pointer aligned.
    // If no more bytes, we have a V1/test manifest with only 6 fields.

    if (r.hasMoreData()) {
        // V2 manifest - skip fields 7-17
        try skipUnionLong(r);
        try skipOptionalMap(r);
        try skipOptionalMap(r);
        try skipOptionalMap(r);
        try skipOptionalMap(r);
        try skipOptionalMapBytes(r);
        try skipOptionalMapBytes(r);
        try skipOptionalBytes(r);
        try skipOptionalArray(r);
        try skipOptionalArray(r);
        try skipUnionInt(r);
    }

    return DataFile{
        .content = content,
        .file_path = file_path,
        .file_format = file_format,
        .partition = partition,
        .record_count = record_count,
        .file_size_in_bytes = file_size_in_bytes,
        .block_size_in_bytes = null,
        .column_sizes = null,
        .value_counts = null,
        .null_value_counts = null,
        .nan_value_counts = null,
        .lower_bounds = null,
        .upper_bounds = null,
        .key_metadata = null,
        .split_offsets = null,
        .equality_ids = null,
        .sort_order_id = null,
    };
}

// Helper: Skip a union field with long value (union: null, long)
fn skipUnionLong(r: *reader_mod.AvroReader) !void {
    const union_idx = try r.readLong();
    if (union_idx == 0) return; // null
    _ = try r.readLong(); // consume the long value
}

// Helper: Skip a union field with int value (union: null, int)
fn skipUnionInt(r: *reader_mod.AvroReader) !void {
    const union_idx = try r.readLong();
    if (union_idx == 0) return; // null
    _ = try r.readInt(); // consume the int value
}

// Helper: Skip optional bytes (union: null, bytes)
fn skipOptionalBytes(r: *reader_mod.AvroReader) !void {
    const union_idx = try r.readLong();
    if (union_idx == 0) return; // null
    _ = try r.readBytes(); // consume the bytes
}

// Helper: Skip an optional Avro map (union: null, map<int, long>)
fn skipOptionalMap(r: *reader_mod.AvroReader) !void {
    const union_idx = try r.readLong();
    if (union_idx == 0) return; // null

    // Map present - consume all entries
    var count = try r.readLong();
    while (count != 0) {
        if (count < 0) {
            // Block count with size - skip the size
            _ = try r.readLong();
            count = -count;
        }

        var i: i64 = 0;
        while (i < count) : (i += 1) {
            // Skip key (int) and value (long)
            _ = try r.readInt();
            _ = try r.readLong();
        }

        count = try r.readLong();
    }
}

// Helper: Skip an optional Avro map with bytes values (union: null, map<int, bytes>)
fn skipOptionalMapBytes(r: *reader_mod.AvroReader) !void {
    const union_idx = try r.readLong();
    if (union_idx == 0) return; // null

    // Map present - consume all entries
    var count = try r.readLong();
    while (count != 0) {
        if (count < 0) {
            // Block count with size - skip the size
            _ = try r.readLong();
            count = -count;
        }

        var i: i64 = 0;
        while (i < count) : (i += 1) {
            // Skip key (int) and value (bytes)
            _ = try r.readInt();
            _ = try r.readBytes();
        }

        count = try r.readLong();
    }
}

// Helper: Skip an optional Avro array (union: null, array<long>)
fn skipOptionalArray(r: *reader_mod.AvroReader) !void {
    const union_idx = try r.readLong();
    if (union_idx == 0) return; // null

    // Array present - consume all items
    var count = try r.readLong();
    while (count != 0) {
        if (count < 0) {
            // Block count with size - skip the size
            _ = try r.readLong();
            count = -count;
        }

        var i: i64 = 0;
        while (i < count) : (i += 1) {
            _ = try r.readLong();
        }

        count = try r.readLong();
    }
}

pub fn listFiles(allocator: std.mem.Allocator, path: []const u8) !FileList {
    // Use memory mapping for zero-copy file access
    var mapped = try file_mapper.MappedFile.init(path);
    defer mapped.deinit();

    // Parse Avro container with memory-mapped data
    var avro_reader = reader_mod.AvroReader.init(allocator, mapped.data);

    var header = try avro_reader.readHeader();
    defer header.deinit();

    // Collect all files
    var files_list = std.ArrayList(FileInfo){};
    errdefer files_list.deinit(allocator);

    while (try avro_reader.readDataBlock(header.sync_marker)) |block| {
        var block_reader = reader_mod.AvroReader.init(allocator, block.data);

        var i: i64 = 0;
        while (i < block.count) : (i += 1) {
            const entry = try parseManifestEntry(&block_reader);

            // Duplicate strings since they reference the file_data buffer
            const path_copy = try allocator.dupe(u8, entry.data_file.file_path);
            const partition_copy = if (entry.data_file.partition) |p|
                try allocator.dupe(u8, p)
            else
                null;

            try files_list.append(allocator, FileInfo{
                .path = path_copy,
                .size = @as(u64, @intCast(entry.data_file.file_size_in_bytes)),
                .record_count = entry.data_file.record_count,
                .partition = partition_copy,
            });
        }
    }

    return FileList{
        .files = try files_list.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}
