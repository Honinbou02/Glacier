/// RLE (Run-Length Encoding) / Bit-Packing Hybrid decoder for Parquet
/// Spec: https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
const std = @import("std");

/// RLE Hybrid Decoder with PROPER bit-packing support
/// Supports both RLE runs and bit-packed runs
pub const RLEDecoder = struct {
    data: []const u8,
    pos: usize,
    bit_width: u5,

    pub fn init(data: []const u8, bit_width: u5) RLEDecoder {
        return .{
            .data = data,
            .pos = 0,
            .bit_width = bit_width,
        };
    }

    /// Read a varint (unsigned LEB128)
    fn readVarint(self: *RLEDecoder) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;

        while (self.pos < self.data.len) {
            const byte = self.data[self.pos];
            self.pos += 1;

            result |= (@as(u64, byte & 0x7F) << shift);

            if ((byte & 0x80) == 0) {
                return result;
            }

            shift += 7;
            if (shift >= 64) return error.VarintOverflow;
        }

        return error.UnexpectedEndOfData;
    }

    /// Read a single value with proper bit-packing
    /// This implements CORRECT bit-level reading for Parquet RLE
    fn readBitPackedValue(self: *RLEDecoder, bit_offset: *usize) !u64 {
        if (self.bit_width == 0) return 0;

        const total_bits_needed = self.bit_width;
        var result: u64 = 0;
        var bits_read: u5 = 0;

        while (bits_read < total_bits_needed) {
            const byte_pos = self.pos + (bit_offset.* / 8);
            const bit_pos_in_byte: u3 = @intCast(bit_offset.* % 8);

            if (byte_pos >= self.data.len) return error.UnexpectedEndOfData;

            const bits_available_in_byte: u4 = @intCast(8 - @as(u4, bit_pos_in_byte));
            const bits_to_read: u5 = @min(total_bits_needed - bits_read, @as(u5, bits_available_in_byte));

            // Extract bits from current byte
            const byte = self.data[byte_pos];
            // Handle edge case: when bits_to_read=8, use 0xFF instead of (1 << 8) - 1
            const mask: u8 = if (bits_to_read >= 8) 0xFF else (@as(u8, 1) << @intCast(bits_to_read)) - 1;
            const extracted = (byte >> bit_pos_in_byte) & mask;

            // Add to result
            result |= @as(u64, extracted) << bits_read;

            bits_read += bits_to_read;
            bit_offset.* += bits_to_read;
        }

        return result;
    }

    /// Read value for RLE run
    fn readRLEValue(self: *RLEDecoder) !u64 {
        if (self.bit_width == 0) return 0;

        // Read bit-packed value (not byte-aligned)
        var bit_offset: usize = 0;
        const value = try self.readBitPackedValue(&bit_offset);

        // Advance position by the bits consumed
        const bits_consumed = self.bit_width;
        const bytes_consumed = (bits_consumed + 7) / 8;
        self.pos += bytes_consumed;

        return value;
    }

    /// Decode RLE/bit-packed hybrid encoded data
    /// Returns array of decoded integer values
    pub fn decodeAll(
        self: *RLEDecoder,
        allocator: std.mem.Allocator,
        num_values: usize,
    ) ![]i64 {
        var values = try allocator.alloc(i64, num_values);
        errdefer allocator.free(values);

        var offset: usize = 0;

        while (offset < num_values) {
            if (self.pos >= self.data.len) {
                return error.UnexpectedEndOfData;
            }

            // Read run header (varint)
            const header = try self.readVarint();

            // Check if it's RLE run or bit-packed run
            // Per Parquet spec:
            // - Bit-packed: header = (count << 1) | 1  (LSB = 1)
            // - RLE:        header = (count << 1)      (LSB = 0)
            const is_rle = (header & 1) == 0;
            const count = header >> 1;

            if (count == 0) continue; // Skip empty runs

            if (is_rle) {
                // RLE run: [count] repeated values of [value]
                const value = try self.readRLEValue();

                var i: usize = 0;
                while (i < count and offset < num_values) : (i += 1) {
                    values[offset] = @intCast(value);
                    offset += 1;
                }
            } else {
                // Bit-packed run: count is NUMBER OF GROUPS (each group = 8 values)
                const num_vals_in_run = count * 8;

                // Calculate actual bytes available
                const bytes_available = self.data.len - self.pos;

                // Read bit-packed values (only up to num_values total!)
                var bit_offset: usize = 0;
                var i: usize = 0;
                const values_to_read = @min(num_vals_in_run, num_values - offset);

                while (i < values_to_read) : (i += 1) {
                    const value = self.readBitPackedValue(&bit_offset) catch break;
                    values[offset] = @intCast(value);
                    offset += 1;
                }

                // Advance byte position past the ACTUAL data consumed
                const actual_bits_consumed = i * @as(usize, self.bit_width);
                const actual_bytes_consumed = (actual_bits_consumed + 7) / 8;
                const safe_bytes = @min(actual_bytes_consumed, bytes_available);
                self.pos += safe_bytes;
            }
        }

        return values;
    }
};

// ═════════════════════════════════════════════════════════════════════════════
// TESTS
// ═════════════════════════════════════════════════════════════════════════════

test "RLE decoder - simple RLE run" {
    const allocator = std.testing.allocator;

    // RLE run: 3 values of 42
    // Header: (3 << 1) | 0 = 6 (LSB=0 for RLE!)
    // Value: 42 (in 8 bits)
    const data = [_]u8{ 6, 42 };

    var decoder = RLEDecoder.init(&data, 8);
    const values = try decoder.decodeAll(allocator, 3);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 3), values.len);
    try std.testing.expectEqual(@as(i64, 42), values[0]);
    try std.testing.expectEqual(@as(i64, 42), values[1]);
    try std.testing.expectEqual(@as(i64, 42), values[2]);
}

test "RLE decoder - multiple RLE runs" {
    const allocator = std.testing.allocator;

    // First run: 2 values of 10
    // Header: (2 << 1) | 0 = 4 (LSB=0 for RLE!)
    // Value: 10
    // Second run: 3 values of 20
    // Header: (3 << 1) | 0 = 6 (LSB=0 for RLE!)
    // Value: 20
    const data = [_]u8{ 4, 10, 6, 20 };

    var decoder = RLEDecoder.init(&data, 8);
    const values = try decoder.decodeAll(allocator, 5);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);
    try std.testing.expectEqual(@as(i64, 10), values[0]);
    try std.testing.expectEqual(@as(i64, 10), values[1]);
    try std.testing.expectEqual(@as(i64, 20), values[2]);
    try std.testing.expectEqual(@as(i64, 20), values[3]);
    try std.testing.expectEqual(@as(i64, 20), values[4]);
}

test "RLE decoder - bit-packed run with bit_width=3" {
    const allocator = std.testing.allocator;

    // Bit-packed run: 8 values with bit_width=3
    // Header: (1 << 1) | 1 = 3 (LSB=1 for bit-packed!)
    // 1 group of 8 values packed in 3 bits each = 24 bits = 3 bytes
    // Using simple data: all zeros for testing
    const data = [_]u8{ 3, 0, 0, 0 }; // Header + 3 bytes of zeros = 8 values of 0

    var decoder = RLEDecoder.init(&data, 3);
    const values = try decoder.decodeAll(allocator, 8);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 8), values.len);
    for (values) |v| {
        try std.testing.expectEqual(@as(i64, 0), v);
    }
}

test "RLE decoder - error on insufficient data" {
    const allocator = std.testing.allocator;

    // RLE run claiming 5 values but only providing 1 byte
    const data = [_]u8{ 10 }; // Header: (5 << 1) | 0 = 10 (LSB=0 for RLE!), but no value byte

    var decoder = RLEDecoder.init(&data, 8);
    const result = decoder.decodeAll(allocator, 5);

    try std.testing.expectError(error.UnexpectedEndOfData, result);
}
