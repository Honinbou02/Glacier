// ═══════════════════════════════════════════════════════════════════════════
// PARQUET COLUMN READER (DREMEL ALGORITHM)
// ═══════════════════════════════════════════════════════════════════════════
//
// The Dremel algorithm synchronizes three separate streams:
//   1. Repetition Levels (RL) - tracks nested list boundaries
//   2. Definition Levels (DL) - tracks nullability
//   3. Values - actual data values
//
// THE PROBLEM:
//   In Parquet, NULL values don't appear in the Values stream!
//   You MUST read Definition Level first to know if a value exists.
//
//   If def_level < max_def_level: Value is NULL, DON'T read from Values stream
//   If def_level == max_def_level: Value exists, read from Values stream
//
// EXAMPLE (nullable column, max_def_level = 1):
//   Row 0: value = 100  → DL=1, read from Values → get 100
//   Row 1: value = NULL → DL=0, skip Values stream
//   Row 2: value = 200  → DL=1, read from Values → get 200
//   Row 3: value = NULL → DL=0, skip Values stream
//
//   Definition Levels: [1, 0, 1, 0]
//   Values stream:     [100, 200]  ← Only 2 values, not 4!
//
// If you naively read 4 values from the Values stream, you'll read:
//   [100, 200, <garbage>, <garbage>] ← WRONG!
//
// DATA PAGE VERSIONS:
//   V1 (most common): Definition/Repetition levels have 4-byte length prefix
//   V2 (newer): No prefix, size is in page header, RLE encoding is pure
//
// REFERENCES:
//   - Dremel Paper: https://research.google/pubs/pub36632/
//   - Parquet Format: https://github.com/apache/parquet-format/blob/master/Encodings.md
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const encoding = @import("encoding.zig");

/// Parquet data page version
pub const DataPageVersion = enum {
    /// V1: Definition/Repetition levels have 4-byte length prefix
    v1,
    /// V2: No length prefix, size is in page header
    v2,
};

/// Parquet data page contains 3 synchronized streams
pub const DataPage = struct {
    /// Definition levels (RLE encoded)
    /// V1: 4-byte length prefix + RLE data
    /// V2: Pure RLE data (no prefix)
    definition_levels: []const u8,

    /// Repetition levels (RLE encoded)
    /// V1: 4-byte length prefix + RLE data
    /// V2: Pure RLE data (no prefix)
    repetition_levels: []const u8,

    /// Actual values (encoding depends on column type)
    values: []const u8,

    /// Number of values in this page
    num_values: u32,

    /// Max definition level for this column
    max_def_level: u16,

    /// Max repetition level for this column
    max_rep_level: u16,

    /// Page format version
    version: DataPageVersion = .v1,
};

/// Column reader that implements the Dremel algorithm
/// Synchronizes Definition Levels, Repetition Levels, and Values
pub const ColumnReader = struct {
    /// Definition level decoder
    def_decoder: ?encoding.RLEDecoder,

    /// Repetition level decoder
    rep_decoder: ?encoding.RLEDecoder,

    /// Values remaining in current page
    values_remaining: u32,

    /// Max definition level (0 = required, 1 = optional, >1 = nested)
    max_def_level: u16,

    /// Max repetition level (0 = not repeated, >0 = in list)
    max_rep_level: u16,

    const Self = @This();

    pub fn init(page: DataPage) !Self {
        // Calculate bit widths for RLE decoders
        const def_bit_width = calculateBitWidth(page.max_def_level);
        const rep_bit_width = calculateBitWidth(page.max_rep_level);

        // Initialize decoders based on page version
        var def_decoder: ?encoding.RLEDecoder = null;
        if (page.max_def_level > 0) {
            const def_data = switch (page.version) {
                .v1 => blk: {
                    // V1: Skip 4-byte length prefix
                    if (page.definition_levels.len < 4) {
                        return error.InvalidDefinitionLevels;
                    }
                    break :blk page.definition_levels[4..];
                },
                .v2 => blk: {
                    // V2: No prefix, use data as-is
                    break :blk page.definition_levels;
                },
            };
            def_decoder = encoding.RLEDecoder.init(def_data, def_bit_width);
        }

        var rep_decoder: ?encoding.RLEDecoder = null;
        if (page.max_rep_level > 0) {
            const rep_data = switch (page.version) {
                .v1 => blk: {
                    // V1: Skip 4-byte length prefix
                    if (page.repetition_levels.len < 4) {
                        return error.InvalidRepetitionLevels;
                    }
                    break :blk page.repetition_levels[4..];
                },
                .v2 => blk: {
                    // V2: No prefix, use data as-is
                    break :blk page.repetition_levels;
                },
            };
            rep_decoder = encoding.RLEDecoder.init(rep_data, rep_bit_width);
        }

        return Self{
            .def_decoder = def_decoder,
            .rep_decoder = rep_decoder,
            .values_remaining = page.num_values,
            .max_def_level = page.max_def_level,
            .max_rep_level = page.max_rep_level,
        };
    }

    /// Read next value with its definition and repetition levels
    /// Returns: { def_level, rep_level, has_value }
    /// If has_value = false, the value is NULL and you should NOT read from Values stream
    pub fn readLevels(self: *Self) !LevelResult {
        if (self.values_remaining == 0) {
            return error.NoMoreValues;
        }

        // Read definition level
        const def_level: u16 = if (self.max_def_level > 0) blk: {
            if (self.def_decoder) |*decoder| {
                const val = try decoder.readValue();
                break :blk @intCast(val);
            } else {
                return error.InvalidState;
            }
        } else blk: {
            // No definition levels means column is required (never null)
            break :blk 0;
        };

        // Read repetition level
        const rep_level: u16 = if (self.max_rep_level > 0) blk: {
            if (self.rep_decoder) |*decoder| {
                const val = try decoder.readValue();
                break :blk @intCast(val);
            } else {
                return error.InvalidState;
            }
        } else blk: {
            // No repetition levels means not a repeated field
            break :blk 0;
        };

        self.values_remaining -= 1;

        // Determine if value exists
        // Value exists only if definition level equals max definition level
        const has_value = (def_level == self.max_def_level);

        return LevelResult{
            .def_level = def_level,
            .rep_level = rep_level,
            .has_value = has_value,
        };
    }

    /// Read multiple levels into buffers
    pub fn readLevelsBatch(self: *Self, output: []LevelResult) !usize {
        var count: usize = 0;
        for (output) |*result| {
            if (self.values_remaining == 0) break;

            result.* = try self.readLevels();
            count += 1;
        }
        return count;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.values_remaining == 0;
    }
};

/// Result of reading definition and repetition levels
pub const LevelResult = struct {
    /// Definition level (0 to max_def_level)
    def_level: u16,

    /// Repetition level (0 to max_rep_level)
    rep_level: u16,

    /// Whether a value exists in the Values stream
    /// false means NULL - DON'T read from Values stream
    has_value: bool,
};

/// Calculate minimum bit width to represent a value
fn calculateBitWidth(max_value: u16) u5 {
    if (max_value == 0) return 0;

    var bits: u5 = 0;
    var val = max_value;
    while (val > 0) : (val >>= 1) {
        bits += 1;
    }
    return bits;
}

// ═══════════════════════════════════════════════════════════════════════════
// MATERIALIZATION HELPER
// ═══════════════════════════════════════════════════════════════════════════
//
// This helper reads from both the ColumnReader and a separate Values decoder
// to materialize the actual column data with nulls in the right places.
//
// ═══════════════════════════════════════════════════════════════════════════

/// Helper to materialize values with proper null handling
/// T is the value type (i32, i64, f64, etc.)
pub fn ValueMaterializer(comptime T: type) type {
    return struct {
        column_reader: *ColumnReader,

        const Self = @This();

        /// Read next value, consulting definition levels
        /// Returns null if definition level indicates null value
        pub fn readValue(self: *Self, value_decoder: anytype) !?T {
            const levels = try self.column_reader.readLevels();

            if (!levels.has_value) {
                // Value is NULL, don't read from value decoder
                return null;
            }

            // Value exists, read from decoder
            const value = try value_decoder.readValue();
            return value;
        }

        /// Read multiple values into a buffer
        /// Returns number of values read (may be less than buffer size)
        pub fn readValues(
            self: *Self,
            value_decoder: anytype,
            output: []?T,
        ) !usize {
            var count: usize = 0;

            for (output) |*slot| {
                if (self.column_reader.isAtEnd()) break;

                slot.* = try self.readValue(value_decoder);
                count += 1;
            }

            return count;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "ColumnReader - required column (no nulls)" {
    // Required column has max_def_level = 0, no definition levels stream
    // All values exist in Values stream

    const page = DataPage{
        .definition_levels = &.{},
        .repetition_levels = &.{},
        .values = &.{}, // We don't test values here, just levels
        .num_values = 3,
        .max_def_level = 0, // Required column
        .max_rep_level = 0,
    };

    var reader = try ColumnReader.init(page);

    // For required columns, all values should exist
    const result1 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), result1.def_level);
    try std.testing.expect(result1.has_value);

    const result2 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), result2.def_level);
    try std.testing.expect(result2.has_value);

    const result3 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), result3.def_level);
    try std.testing.expect(result3.has_value);
}

test "ColumnReader - nullable column with nulls" {
    // Nullable column: max_def_level = 1
    // Definition levels: [1, 0, 1, 0]
    //   1 = value exists
    //   0 = value is NULL
    //
    // Values stream should only have 2 values (for the two 1s)

    // Encode definition levels as RLE
    // Format: 4-byte length prefix + RLE data
    // We'll use a bit-packed run: 4 values, 1 bit each
    // Header: (1 << 1) | 1 = 3 (1 group of 8, but we only use 4)
    // Values: [1, 0, 1, 0] packed in 1 bit each = 0b0101 = 0x05 (LSB first)
    const def_levels = [_]u8{
        0x04, 0x00, 0x00, 0x00, // length prefix (4 bytes of RLE data)
        0x03, // header: bit-packed run, 1 group
        0x05, // bit-packed data: 1,0,1,0 in LSB-first order
        0x00, // padding to reach 4-byte RLE data
        0x00,
    };

    const page = DataPage{
        .definition_levels = &def_levels,
        .repetition_levels = &.{},
        .values = &.{},
        .num_values = 4,
        .max_def_level = 1, // Nullable column
        .max_rep_level = 0,
    };

    var reader = try ColumnReader.init(page);

    // Read value 0: def_level=1, value exists
    const r0 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 1), r0.def_level);
    try std.testing.expect(r0.has_value);

    // Read value 1: def_level=0, NULL
    const r1 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), r1.def_level);
    try std.testing.expect(!r1.has_value);

    // Read value 2: def_level=1, value exists
    const r2 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 1), r2.def_level);
    try std.testing.expect(r2.has_value);

    // Read value 3: def_level=0, NULL
    const r3 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), r3.def_level);
    try std.testing.expect(!r3.has_value);
}

test "calculateBitWidth" {
    try std.testing.expectEqual(@as(u5, 0), calculateBitWidth(0));
    try std.testing.expectEqual(@as(u5, 1), calculateBitWidth(1));
    try std.testing.expectEqual(@as(u5, 2), calculateBitWidth(2));
    try std.testing.expectEqual(@as(u5, 2), calculateBitWidth(3));
    try std.testing.expectEqual(@as(u5, 3), calculateBitWidth(4));
    try std.testing.expectEqual(@as(u5, 3), calculateBitWidth(7));
    try std.testing.expectEqual(@as(u5, 4), calculateBitWidth(8));
    try std.testing.expectEqual(@as(u5, 4), calculateBitWidth(15));
    try std.testing.expectEqual(@as(u5, 5), calculateBitWidth(16));
}

test "ValueMaterializer - nullable i32 column" {
    // Test complete integration: Definition Levels + Value Decoder
    //
    // Column values (logical): [100, NULL, 200, NULL]
    // Definition levels:       [1, 0, 1, 0]
    // Values stream (only non-nulls): [100, 200]
    //
    // This is THE KEY POINT: Values stream only has 2 values, not 4!

    // Setup definition levels (RLE encoded)
    const def_levels = [_]u8{
        0x04, 0x00, 0x00, 0x00, // length prefix
        0x03, // bit-packed run header
        0x05, // data: [1,0,1,0]
        0x00, // padding
        0x00,
    };

    const page = DataPage{
        .definition_levels = &def_levels,
        .repetition_levels = &.{},
        .values = &.{}, // Values handled separately
        .num_values = 4,
        .max_def_level = 1,
        .max_rep_level = 0,
    };

    var column_reader = try ColumnReader.init(page);

    // Mock value decoder that returns 100, then 200
    const MockI32Decoder = struct {
        values: [2]i32 = .{ 100, 200 },
        pos: usize = 0,

        pub fn readValue(self: *@This()) !i32 {
            if (self.pos >= self.values.len) return error.NoMoreValues;
            const val = self.values[self.pos];
            self.pos += 1;
            return val;
        }
    };

    var value_decoder = MockI32Decoder{};
    var materializer = ValueMaterializer(i32){ .column_reader = &column_reader };

    // Read value 0: should be 100
    const v0 = try materializer.readValue(&value_decoder);
    try std.testing.expectEqual(@as(i32, 100), v0.?);

    // Read value 1: should be NULL
    const v1 = try materializer.readValue(&value_decoder);
    try std.testing.expect(v1 == null);

    // Read value 2: should be 200
    const v2 = try materializer.readValue(&value_decoder);
    try std.testing.expectEqual(@as(i32, 200), v2.?);

    // Read value 3: should be NULL
    const v3 = try materializer.readValue(&value_decoder);
    try std.testing.expect(v3 == null);

    // Verify value decoder consumed exactly 2 values (not 4!)
    try std.testing.expectEqual(@as(usize, 2), value_decoder.pos);
}

test "ColumnReader - DataPage V2 format" {
    // DataPage V2 doesn't have 4-byte length prefix
    // The RLE data starts immediately

    // RLE encoded definition levels: [1, 0, 1, 0]
    // Without the 4-byte prefix
    const def_levels = [_]u8{
        0x03, // header: bit-packed run, 1 group
        0x05, // bit-packed data: 1,0,1,0
        0x00, // padding
    };

    const page = DataPage{
        .definition_levels = &def_levels,
        .repetition_levels = &.{},
        .values = &.{},
        .num_values = 4,
        .max_def_level = 1,
        .max_rep_level = 0,
        .version = .v2, // Using V2 format
    };

    var reader = try ColumnReader.init(page);

    // Should work the same as V1, just without the prefix
    const r0 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 1), r0.def_level);
    try std.testing.expect(r0.has_value);

    const r1 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), r1.def_level);
    try std.testing.expect(!r1.has_value);

    const r2 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 1), r2.def_level);
    try std.testing.expect(r2.has_value);

    const r3 = try reader.readLevels();
    try std.testing.expectEqual(@as(u16, 0), r3.def_level);
    try std.testing.expect(!r3.has_value);
}

test "Complete integration - INT32 column with nulls and PLAIN encoding" {
    const value_decoder = @import("value_decoder.zig");

    // Column data: [100, NULL, 200, NULL, 300]
    // Definition levels: [1, 0, 1, 0, 1]
    // Values (PLAIN INT32): [100, 200, 300] (3 values, not 5!)

    // Definition levels (RLE encoded, V1 format with 4-byte prefix)
    const def_levels = [_]u8{
        0x04, 0x00, 0x00, 0x00, // length prefix
        0x03, // bit-packed run header
        0x15, // bits: 1,0,1,0,1 in LSB-first order (0b00010101)
        0x00, // padding
        0x00,
    };

    // Values: INT32 PLAIN encoding [100, 200, 300]
    const values = [_]u8{
        0x64, 0x00, 0x00, 0x00, // 100
        0xC8, 0x00, 0x00, 0x00, // 200
        0x2C, 0x01, 0x00, 0x00, // 300
    };

    const page = DataPage{
        .definition_levels = &def_levels,
        .repetition_levels = &.{},
        .values = &values,
        .num_values = 5,
        .max_def_level = 1,
        .max_rep_level = 0,
        .version = .v1,
    };

    var column_reader = try ColumnReader.init(page);
    var int32_decoder = value_decoder.PlainInt32Decoder.init(page.values);
    var materializer = ValueMaterializer(i32){ .column_reader = &column_reader };

    // Read all 5 values
    const v0 = try materializer.readValue(&int32_decoder);
    try std.testing.expectEqual(@as(i32, 100), v0.?);

    const v1 = try materializer.readValue(&int32_decoder);
    try std.testing.expect(v1 == null);

    const v2 = try materializer.readValue(&int32_decoder);
    try std.testing.expectEqual(@as(i32, 200), v2.?);

    const v3 = try materializer.readValue(&int32_decoder);
    try std.testing.expect(v3 == null);

    const v4 = try materializer.readValue(&int32_decoder);
    try std.testing.expectEqual(@as(i32, 300), v4.?);

    // Verify int32_decoder consumed exactly 3 values (not 5!)
    try std.testing.expect(int32_decoder.isAtEnd());
}
