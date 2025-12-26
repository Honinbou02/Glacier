const std = @import("std");
const types = @import("avro_types.zig");

pub const AvroReader = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    pos: usize,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) AvroReader {
        return .{
            .allocator = allocator,
            .data = data,
            .pos = 0,
        };
    }

    /// Check if we've reached the end of the data
    pub fn hasMoreData(self: *AvroReader) bool {
        return self.pos < self.data.len;
    }

    pub fn readHeader(self: *AvroReader) !types.Header {
        // Read magic bytes
        if (self.pos + 4 > self.data.len) return error.UnexpectedEOF;
        const magic = self.data[self.pos..][0..4].*;
        self.pos += 4;

        if (!std.mem.eql(u8, &magic, &types.AVRO_MAGIC)) {
            return error.InvalidAvroFile;
        }

        // Read metadata map
        var metadata = std.StringHashMap([]const u8).init(self.allocator);
        errdefer metadata.deinit();

        const num_entries = try self.readLong();
        if (num_entries < 0) return error.InvalidMetadata;

        var i: i64 = 0;
        while (i < num_entries) : (i += 1) {
            const key = try self.readString();
            const value = try self.readBytes();
            try metadata.put(key, value);
        }

        // Read end of map marker (0)
        const end_marker = try self.readLong();
        if (end_marker != 0) return error.InvalidMetadata;

        // Read sync marker
        if (self.pos + 16 > self.data.len) return error.UnexpectedEOF;
        const sync_marker = self.data[self.pos..][0..16].*;
        self.pos += 16;

        return types.Header{
            .magic = magic,
            .metadata = metadata,
            .sync_marker = sync_marker,
        };
    }

    pub fn readDataBlock(self: *AvroReader, expected_sync: [16]u8) !?types.DataBlock {
        if (self.pos >= self.data.len) return null;

        // Read block count (number of objects in this block)
        const count = try self.readLong();
        if (count == 0) return null;

        // Read block size (in bytes)
        const size = try self.readLong();
        if (size < 0) return error.InvalidBlockSize;

        const size_usize = @as(usize, @intCast(size));
        if (self.pos + size_usize > self.data.len) return error.UnexpectedEOF;

        // Read block data
        const data = self.data[self.pos..][0..size_usize];
        self.pos += size_usize;

        // Read sync marker
        if (self.pos + 16 > self.data.len) return error.UnexpectedEOF;
        const sync_marker = self.data[self.pos..][0..16].*;
        self.pos += 16;

        // Verify sync marker
        if (!std.mem.eql(u8, &sync_marker, &expected_sync)) {
            return error.SyncMarkerMismatch;
        }

        return types.DataBlock{
            .count = count,
            .size = size,
            .data = data,
            .sync_marker = sync_marker,
        };
    }

    // Read variable-length integer (zigzag encoded)
    pub fn readLong(self: *AvroReader) !i64 {
        var result: u64 = 0;
        var shift: u6 = 0;

        while (true) {
            if (self.pos >= self.data.len) return error.UnexpectedEOF;
            const byte = self.data[self.pos];
            self.pos += 1;

            result |= @as(u64, byte & 0x7F) << shift;

            if ((byte & 0x80) == 0) break;
            shift += 7;
            if (shift >= 64) return error.IntegerOverflow;
        }

        // Decode zigzag
        const zigzag = @as(i64, @bitCast(result));
        return (zigzag >> 1) ^ -(zigzag & 1);
    }

    pub fn readString(self: *AvroReader) ![]const u8 {
        const len = try self.readLong();
        if (len < 0) return error.InvalidStringLength;

        const len_usize = @as(usize, @intCast(len));
        if (self.pos + len_usize > self.data.len) return error.UnexpectedEOF;

        const str = self.data[self.pos..][0..len_usize];
        self.pos += len_usize;
        return str;
    }

    pub fn readBytes(self: *AvroReader) ![]const u8 {
        return self.readString();
    }

    pub fn readInt(self: *AvroReader) !i32 {
        const val = try self.readLong();
        if (val < std.math.minInt(i32) or val > std.math.maxInt(i32)) {
            return error.IntegerOverflow;
        }
        return @as(i32, @intCast(val));
    }

    pub fn readBoolean(self: *AvroReader) !bool {
        if (self.pos >= self.data.len) return error.UnexpectedEOF;
        const byte = self.data[self.pos];
        self.pos += 1;
        return byte != 0;
    }
};
