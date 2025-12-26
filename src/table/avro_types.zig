const std = @import("std");

// Avro magic bytes: 'O', 'b', 'j', 1
pub const AVRO_MAGIC: [4]u8 = .{ 'O', 'b', 'j', 1 };

pub const Header = struct {
    magic: [4]u8,
    metadata: std.StringHashMap([]const u8),
    sync_marker: [16]u8,

    pub fn deinit(self: *Header) void {
        self.metadata.deinit();
    }
};

pub const Schema = struct {
    raw: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: Schema) void {
        self.allocator.free(self.raw);
    }
};

pub const DataBlock = struct {
    count: i64,
    size: i64,
    data: []const u8,
    sync_marker: [16]u8,
};
