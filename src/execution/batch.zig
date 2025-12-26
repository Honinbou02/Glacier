// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Vectorized Batch Processing
// ═══════════════════════════════════════════════════════════════════════════
//
// Implements Arrow-style columnar batches for SIMD-optimized query execution.
//
// Key Concepts:
// - Fixed-size batches (default 1024 rows) for CPU cache efficiency
// - Columnar layout: all values for column N stored contiguously
// - Validity bitmaps for NULL handling (1 bit per row)
// - Zero-copy slicing for filter results
//
// Memory Layout Example (3 columns, 4 rows):
//
//   Column 0 (INT64):  [1, 2, 3, 4]           <- 32 bytes (8 * 4)
//   Column 1 (STRING): [ptr1, ptr2, ptr3, ...]  <- pointers
//   Column 2 (INT32):  [10, 20, NULL, 40]     <- 16 bytes (4 * 4)
//   Validity bitmap:   [0b1011]               <- 1 byte (bit 2 = 0 for NULL)
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");

/// Default batch size - tuned for L1 cache (32KB typical)
/// With 8-byte values: 1024 rows * 8 bytes = 8KB per column
pub const DEFAULT_BATCH_SIZE = 1024;

/// Physical data type for a column
pub const DataType = enum {
    boolean,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
    float32,
    float64,
    string,
    binary,
    date32, // Days since Unix epoch
    timestamp, // Microseconds since Unix epoch

    /// Get size in bytes for fixed-width types
    pub fn byteSize(self: DataType) ?usize {
        return switch (self) {
            .int8, .uint8, .boolean => 1,
            .int16, .uint16 => 2,
            .int32, .uint32, .float32, .date32 => 4,
            .int64, .uint64, .float64, .timestamp => 8,
            .string, .binary => null, // Variable length
        };
    }

    /// Returns true if type is fixed-width
    pub fn isFixedWidth(self: DataType) bool {
        return self.byteSize() != null;
    }
};

/// Column metadata
pub const ColumnSchema = struct {
    name: []const u8,
    data_type: DataType,
    nullable: bool,
};

/// Batch schema (table structure)
pub const Schema = struct {
    columns: []const ColumnSchema,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, columns: []const ColumnSchema) !Schema {
        const columns_copy = try allocator.dupe(ColumnSchema, columns);
        return Schema{
            .columns = columns_copy,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Schema) void {
        self.allocator.free(self.columns);
    }

    pub fn columnIndex(self: Schema, name: []const u8) ?usize {
        for (self.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, name)) {
                return i;
            }
        }
        return null;
    }
};

/// Represents a column of data in memory
pub const ColumnVector = struct {
    data_type: DataType,

    /// Raw bytes storing the column values
    data: []u8,

    /// Validity bitmap (1 bit per row, 1 = valid, 0 = null)
    /// Only allocated if column is nullable
    null_bitmap: ?[]u8,

    /// Number of rows in this column
    len: usize,

    pub fn deinit(self: *ColumnVector, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        if (self.null_bitmap) |bitmap| {
            allocator.free(bitmap);
        }
    }

    /// Check if value at index is NULL
    pub fn isNull(self: ColumnVector, index: usize) bool {
        if (self.null_bitmap) |bitmap| {
            const byte_idx = index / 8;
            const bit_idx: u3 = @intCast(index % 8);
            return (bitmap[byte_idx] & (@as(u8, 1) << bit_idx)) == 0;
        }
        return false; // Non-nullable column
    }

    /// Set NULL bit for index
    pub fn setNull(self: *ColumnVector, index: usize, is_null: bool) void {
        if (self.null_bitmap) |bitmap| {
            const byte_idx = index / 8;
            const bit_idx: u3 = @intCast(index % 8);
            if (is_null) {
                bitmap[byte_idx] &= ~(@as(u8, 1) << bit_idx);
            } else {
                bitmap[byte_idx] |= (@as(u8, 1) << bit_idx);
            }
        }
    }

    /// Get typed slice for fixed-width columns
    pub fn asSlice(self: ColumnVector, comptime T: type) []T {
        const elem_size = @sizeOf(T);
        const count = self.data.len / elem_size;
        return @as([*]T, @ptrCast(@alignCast(self.data.ptr)))[0..count];
    }

    /// Get value at index (returns null if NULL)
    pub fn getValue(self: ColumnVector, comptime T: type, index: usize) ?T {
        if (self.isNull(index)) return null;
        const slice = self.asSlice(T);
        return slice[index];
    }

    /// Set value at index
    pub fn setValue(self: *ColumnVector, comptime T: type, index: usize, value: T) void {
        var slice = self.asSlice(T);
        slice[index] = value;
        self.setNull(index, false);
    }
};

/// A batch of rows represented as columns
pub const Batch = struct {
    row_count: usize,
    columns: []ColumnVector,
    schema: *const Schema,

    /// Optional owned schema - if set, will be freed in deinit()
    /// Used when Batch creates its own schema (e.g., in projection)
    owned_schema: ?*Schema,

    allocator: std.mem.Allocator,

    const Self = @This();

    /// Create a new empty batch - deprecated, use initWithSchema
    pub fn init(allocator: std.mem.Allocator, schema: *const Schema, capacity: usize) !Self {
        return try initWithSchema(allocator, schema, capacity);
    }

    /// Create batch from schema
    pub fn initWithSchema(allocator: std.mem.Allocator, schema: *const Schema, capacity: usize) !Self {
        var columns = try allocator.alloc(ColumnVector, schema.columns.len);
        errdefer allocator.free(columns);

        // Allocate column buffers
        for (schema.columns, 0..) |col_schema, i| {
            const byte_size = col_schema.data_type.byteSize() orelse 16; // Default to pointer size for var-len
            const data = try allocator.alloc(u8, capacity * byte_size);
            errdefer allocator.free(data);

            // Allocate validity bitmap if nullable
            const null_bitmap = if (col_schema.nullable) blk: {
                const bitmap_bytes = (capacity + 7) / 8;
                const bitmap = try allocator.alloc(u8, bitmap_bytes);
                @memset(bitmap, 0xFF); // All valid initially
                break :blk bitmap;
            } else null;

            columns[i] = ColumnVector{
                .data_type = col_schema.data_type,
                .data = data,
                .null_bitmap = null_bitmap,
                .len = 0,
            };
        }

        return Self{
            .row_count = 0,
            .columns = columns,
            .schema = schema,
            .owned_schema = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.columns) |*col| {
            col.deinit(self.allocator);
        }
        self.allocator.free(self.columns);

        // Free owned schema if present
        if (self.owned_schema) |owned| {
            owned.deinit();
            self.allocator.destroy(owned);
        }
    }

    /// Get column by index
    pub fn column(self: *const Self, index: usize) *const ColumnVector {
        return &self.columns[index];
    }

    /// Get mutable column by index
    pub fn columnMut(self: *Self, index: usize) *ColumnVector {
        return &self.columns[index];
    }
};

/// Builder for constructing batches row-by-row
pub const BatchBuilder = struct {
    batch: Batch,
    current_row: usize,
    capacity: usize,

    pub fn init(allocator: std.mem.Allocator, schema: *const Schema, capacity: usize) !BatchBuilder {
        const batch = try Batch.initWithSchema(allocator, schema, capacity);
        return BatchBuilder{
            .batch = batch,
            .current_row = 0,
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *BatchBuilder) void {
        self.batch.deinit();
    }

    /// Add a row to the batch
    /// Returns error.BatchFull if capacity exceeded
    pub fn appendRow(self: *BatchBuilder) !void {
        if (self.current_row >= self.capacity) {
            return error.BatchFull;
        }
        self.current_row += 1;
        self.batch.row_count = self.current_row;
    }

    /// Set value in current row
    pub fn setValue(self: *BatchBuilder, col_idx: usize, comptime T: type, value: T) void {
        self.batch.columns[col_idx].setValue(T, self.current_row - 1, value);
    }

    /// Set NULL in current row
    pub fn setNull(self: *BatchBuilder, col_idx: usize) void {
        self.batch.columns[col_idx].setNull(self.current_row - 1, true);
    }

    /// Finalize and return batch
    pub fn finish(self: *BatchBuilder) Batch {
        return self.batch;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "batch creation" {
    const allocator = std.testing.allocator;

    const cols = [_]ColumnSchema{
        .{ .name = "col1", .data_type = .int64, .nullable = false },
        .{ .name = "col2", .data_type = .int32, .nullable = true },
        .{ .name = "col3", .data_type = .float64, .nullable = false },
        .{ .name = "col4", .data_type = .int64, .nullable = true },
        .{ .name = "col5", .data_type = .int32, .nullable = false },
    };

    var schema = try Schema.init(allocator, &cols);
    defer schema.deinit();

    var batch = try Batch.initWithSchema(allocator, &schema, 1024);
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 0), batch.row_count); // Initially 0
    try std.testing.expectEqual(@as(usize, 5), batch.columns.len);
}

test "DataType byte sizes" {
    try std.testing.expectEqual(@as(?usize, 1), DataType.int8.byteSize());
    try std.testing.expectEqual(@as(?usize, 4), DataType.int32.byteSize());
    try std.testing.expectEqual(@as(?usize, 8), DataType.int64.byteSize());
    try std.testing.expectEqual(@as(?usize, null), DataType.string.byteSize());
}

test "Schema creation and column lookup" {
    const allocator = std.testing.allocator;

    const cols = [_]ColumnSchema{
        .{ .name = "id", .data_type = .int64, .nullable = false },
        .{ .name = "name", .data_type = .string, .nullable = true },
        .{ .name = "age", .data_type = .int32, .nullable = true },
    };

    var schema = try Schema.init(allocator, &cols);
    defer schema.deinit();

    try std.testing.expectEqual(@as(usize, 3), schema.columns.len);
    try std.testing.expectEqual(@as(?usize, 0), schema.columnIndex("id"));
    try std.testing.expectEqual(@as(?usize, 1), schema.columnIndex("name"));
    try std.testing.expectEqual(@as(?usize, 2), schema.columnIndex("age"));
    try std.testing.expectEqual(@as(?usize, null), schema.columnIndex("unknown"));
}

test "BatchBuilder creation and value access" {
    const allocator = std.testing.allocator;

    const cols = [_]ColumnSchema{
        .{ .name = "id", .data_type = .int64, .nullable = false },
        .{ .name = "value", .data_type = .int32, .nullable = true },
    };

    var schema = try Schema.init(allocator, &cols);
    defer schema.deinit();

    var builder = try BatchBuilder.init(allocator, &schema, 10);
    defer builder.deinit();

    // Add first row: id=1, value=100
    try builder.appendRow();
    builder.setValue(0, i64, 1);
    builder.setValue(1, i32, 100);

    // Add second row: id=2, value=NULL
    try builder.appendRow();
    builder.setValue(0, i64, 2);
    builder.setNull(1);

    const batch = builder.finish();

    try std.testing.expectEqual(@as(usize, 2), batch.row_count);

    // Check first row
    const col0 = batch.column(0);
    try std.testing.expectEqual(@as(?i64, 1), col0.getValue(i64, 0));

    const col1 = batch.column(1);
    try std.testing.expectEqual(@as(?i32, 100), col1.getValue(i32, 0));

    // Check second row
    try std.testing.expectEqual(@as(?i64, 2), col0.getValue(i64, 1));
    try std.testing.expectEqual(@as(?i32, null), col1.getValue(i32, 1)); // NULL
}

test "Validity bitmap operations" {
    const allocator = std.testing.allocator;

    const cols = [_]ColumnSchema{
        .{ .name = "value", .data_type = .int32, .nullable = true },
    };

    var schema = try Schema.init(allocator, &cols);
    defer schema.deinit();

    var batch = try Batch.initWithSchema(allocator, &schema, 16);
    defer batch.deinit();

    var col = batch.columnMut(0);

    // Set some values as NULL
    col.setNull(0, false); // Valid
    col.setNull(1, true); // NULL
    col.setNull(2, false); // Valid
    col.setNull(3, true); // NULL
    col.setNull(15, true); // NULL

    // Check validity
    try std.testing.expect(!col.isNull(0));
    try std.testing.expect(col.isNull(1));
    try std.testing.expect(!col.isNull(2));
    try std.testing.expect(col.isNull(3));
    try std.testing.expect(col.isNull(15));
}
