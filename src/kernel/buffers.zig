// ═══════════════════════════════════════════════════════════════════════════
// ZERO-COPY BUFFER MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════
//
// Ring buffers and zero-copy I/O primitives.
//
// GOAL:
//   Network data → Socket → RingBuffer → Parquet Decoder (zero memcpy)
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const errors = @import("../core/errors.zig");

/// Ring buffer for streaming I/O operations
/// Efficient for network reads where data arrives in chunks
pub const RingBuffer = struct {
    data: []u8,
    read_pos: usize,
    write_pos: usize,
    capacity: usize,

    const Self = @This();

    /// Initialize a ring buffer with the given backing storage
    pub fn init(backing_buffer: []u8) Self {
        return .{
            .data = backing_buffer,
            .read_pos = 0,
            .write_pos = 0,
            .capacity = backing_buffer.len,
        };
    }

    /// Get the number of bytes available to read
    pub fn available(self: *const Self) usize {
        if (self.write_pos >= self.read_pos) {
            return self.write_pos - self.read_pos;
        } else {
            return self.capacity - self.read_pos + self.write_pos;
        }
    }

    /// Get the amount of free space for writing
    pub fn free(self: *const Self) usize {
        return self.capacity - self.available() - 1; // -1 to distinguish full from empty
    }

    /// Get a slice of writable space (may wrap around)
    pub fn getWriteSlice(self: *Self) []u8 {
        if (self.write_pos >= self.read_pos) {
            // Can write from write_pos to end, or until read_pos if wrapped
            const end = if (self.read_pos == 0) self.capacity - 1 else self.capacity;
            return self.data[self.write_pos..end];
        } else {
            // Can write from write_pos to read_pos - 1
            return self.data[self.write_pos .. self.read_pos - 1];
        }
    }

    /// Advance the write position after data has been written
    pub fn commitWrite(self: *Self, n: usize) void {
        self.write_pos = (self.write_pos + n) % self.capacity;
    }

    /// Get a slice of readable data (may wrap around)
    pub fn getReadSlice(self: *Self) []u8 {
        if (self.write_pos >= self.read_pos) {
            return self.data[self.read_pos..self.write_pos];
        } else {
            return self.data[self.read_pos..self.capacity];
        }
    }

    /// Advance the read position after data has been consumed
    pub fn commitRead(self: *Self, n: usize) void {
        self.read_pos = (self.read_pos + n) % self.capacity;
    }

    /// Read data from the buffer (copies to destination)
    pub fn read(self: *Self, dest: []u8) usize {
        const readable = self.getReadSlice();
        const to_copy = @min(readable.len, dest.len);

        @memcpy(dest[0..to_copy], readable[0..to_copy]);
        self.commitRead(to_copy);

        return to_copy;
    }

    /// Write data to the buffer (copies from source)
    pub fn write(self: *Self, src: []const u8) usize {
        const writable = self.getWriteSlice();
        const to_copy = @min(writable.len, src.len);

        @memcpy(writable[0..to_copy], src[0..to_copy]);
        self.commitWrite(to_copy);

        return to_copy;
    }

    /// Reset buffer to empty state
    pub fn reset(self: *Self) void {
        self.read_pos = 0;
        self.write_pos = 0;
    }

    /// Check if buffer is empty
    pub fn isEmpty(self: *const Self) bool {
        return self.read_pos == self.write_pos;
    }

    /// Check if buffer is full
    pub fn isFull(self: *const Self) bool {
        return ((self.write_pos + 1) % self.capacity) == self.read_pos;
    }
};

/// Simple growable buffer (for cases where size is not known upfront)
/// Uses arena allocator - no need to free individual items
pub const GrowableBuffer = struct {
    data: []u8,
    len: usize,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .data = &.{},
            .len = 0,
            .allocator = allocator,
        };
    }

    pub fn ensureCapacity(self: *Self, capacity: usize) errors.MemoryError!void {
        if (self.data.len >= capacity) return;

        const new_capacity = @max(capacity, self.data.len * 2);
        const new_data = self.allocator.alloc(u8, new_capacity) catch |err| {
            return errors.fromAllocError(err);
        };

        @memcpy(new_data[0..self.len], self.data[0..self.len]);
        self.data = new_data;
    }

    pub fn append(self: *Self, byte: u8) errors.MemoryError!void {
        try self.ensureCapacity(self.len + 1);
        self.data[self.len] = byte;
        self.len += 1;
    }

    pub fn appendSlice(self: *Self, bytes: []const u8) errors.MemoryError!void {
        try self.ensureCapacity(self.len + bytes.len);
        @memcpy(self.data[self.len .. self.len + bytes.len], bytes);
        self.len += bytes.len;
    }

    pub fn toSlice(self: *const Self) []const u8 {
        return self.data[0..self.len];
    }

    pub fn reset(self: *Self) void {
        self.len = 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "RingBuffer basic operations" {
    var backing: [16]u8 = undefined;
    var rb = RingBuffer.init(&backing);

    try std.testing.expectEqual(@as(usize, 0), rb.available());
    try std.testing.expectEqual(@as(usize, 15), rb.free());
    try std.testing.expect(rb.isEmpty());

    // Write some data
    const written = rb.write("Hello");
    try std.testing.expectEqual(@as(usize, 5), written);
    try std.testing.expectEqual(@as(usize, 5), rb.available());

    // Read it back
    var dest: [10]u8 = undefined;
    const read_count = rb.read(&dest);
    try std.testing.expectEqual(@as(usize, 5), read_count);
    try std.testing.expectEqualSlices(u8, "Hello", dest[0..5]);
    try std.testing.expect(rb.isEmpty());
}

test "RingBuffer wrap around" {
    var backing: [8]u8 = undefined;
    var rb = RingBuffer.init(&backing);

    // Fill buffer almost to capacity
    _ = rb.write("ABCDEF");
    try std.testing.expectEqual(@as(usize, 6), rb.available());

    // Read some
    var dest: [3]u8 = undefined;
    _ = rb.read(&dest);
    try std.testing.expectEqualSlices(u8, "ABC", &dest);
    try std.testing.expectEqual(@as(usize, 3), rb.available());

    // Write more (will wrap around) - only 2 bytes to fit in available space
    const written = rb.write("12");
    try std.testing.expectEqual(@as(usize, 2), written);
    try std.testing.expectEqual(@as(usize, 5), rb.available());

    // Read everything
    var dest2: [10]u8 = undefined;
    const count1 = rb.read(&dest2);
    const count2 = rb.read(dest2[count1..]);
    const total = count1 + count2;

    try std.testing.expectEqual(@as(usize, 5), total);
    try std.testing.expectEqualSlices(u8, "DEF12", dest2[0..5]);
}

test "GrowableBuffer" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var buf = GrowableBuffer.init(arena.allocator());

    try buf.appendSlice("Hello");
    try buf.appendSlice(" ");
    try buf.appendSlice("World");

    try std.testing.expectEqualSlices(u8, "Hello World", buf.toSlice());
}
