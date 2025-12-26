// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Projection (Column Selection)
// ═══════════════════════════════════════════════════════════════════════════
//
// Implements column projection for SELECT clauses.
//
// Example:
//   SELECT id, age FROM table  →  Project columns [0, 2]
//
// This creates a new batch with only the selected columns, avoiding
// unnecessary data movement.
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const batch_mod = @import("batch.zig");
const Batch = batch_mod.Batch;
const Schema = batch_mod.Schema;
const ColumnSchema = batch_mod.ColumnSchema;
const ColumnVector = batch_mod.ColumnVector;
const expr_mod = @import("expr.zig");
const SelectionVector = expr_mod.SelectionVector;

/// Projection operator - selects subset of columns
pub const Projection = struct {
    /// Indices of columns to project
    column_indices: []usize,

    /// Column names (for schema)
    column_names: []const []const u8,

    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        column_names: []const []const u8,
    ) !Projection {
        const column_names_copy = try allocator.dupe([]const u8, column_names);
        const indices = try allocator.alloc(usize, column_names.len);

        return Projection{
            .column_indices = indices,
            .column_names = column_names_copy,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Projection) void {
        self.allocator.free(self.column_indices);
        self.allocator.free(self.column_names);
    }

    /// Project columns from input batch
    pub fn apply(
        self: *Projection,
        input_batch: *const Batch,
        selection: ?*const SelectionVector,
    ) !Batch {
        // Resolve column names to indices
        for (self.column_names, 0..) |col_name, i| {
            self.column_indices[i] = input_batch.schema.columnIndex(col_name) orelse
                return error.ColumnNotFound;
        }

        // Create new schema with selected columns
        var new_columns = try self.allocator.alloc(ColumnSchema, self.column_names.len);
        for (self.column_indices, 0..) |col_idx, i| {
            new_columns[i] = input_batch.schema.columns[col_idx];
        }

        // Allocate schema on heap so Batch can own it
        var new_schema_ptr = try self.allocator.create(Schema);
        errdefer self.allocator.destroy(new_schema_ptr);

        new_schema_ptr.* = try Schema.init(self.allocator, new_columns);
        errdefer new_schema_ptr.deinit();
        self.allocator.free(new_columns);

        // Determine output row count
        const output_row_count = if (selection) |sel| sel.count else input_batch.row_count;

        // Create output batch
        var output_batch = try Batch.initWithSchema(self.allocator, new_schema_ptr, output_row_count);
        errdefer output_batch.deinit();

        // Transfer schema ownership to batch
        output_batch.owned_schema = new_schema_ptr;

        // Copy selected columns
        if (selection) |sel| {
            // With selection: copy only selected rows
            for (self.column_indices, 0..) |col_idx, out_col_idx| {
                const in_col = &input_batch.columns[col_idx];
                var out_col = &output_batch.columns[out_col_idx];

                // Copy selected rows
                for (0..sel.count) |i| {
                    const row_idx = sel.indices[i];
                    if (in_col.isNull(row_idx)) {
                        out_col.setNull(i, true);
                    } else {
                        // Copy based on data type
                        switch (in_col.data_type) {
                            .int64 => {
                                const val = in_col.getValue(i64, row_idx).?;
                                out_col.setValue(i64, i, val);
                            },
                            .int32 => {
                                const val = in_col.getValue(i32, row_idx).?;
                                out_col.setValue(i32, i, val);
                            },
                            .float64 => {
                                const val = in_col.getValue(f64, row_idx).?;
                                out_col.setValue(f64, i, val);
                            },
                            else => return error.UnsupportedType,
                        }
                    }
                }
                out_col.len = sel.count;
            }
            output_batch.row_count = sel.count;
        } else {
            // No selection: copy all rows
            for (self.column_indices, 0..) |col_idx, out_col_idx| {
                const in_col = &input_batch.columns[col_idx];
                var out_col = &output_batch.columns[out_col_idx];

                // Copy all rows
                for (0..input_batch.row_count) |row_idx| {
                    if (in_col.isNull(row_idx)) {
                        out_col.setNull(row_idx, true);
                    } else {
                        switch (in_col.data_type) {
                            .int64 => {
                                const val = in_col.getValue(i64, row_idx).?;
                                out_col.setValue(i64, row_idx, val);
                            },
                            .int32 => {
                                const val = in_col.getValue(i32, row_idx).?;
                                out_col.setValue(i32, row_idx, val);
                            },
                            .float64 => {
                                const val = in_col.getValue(f64, row_idx).?;
                                out_col.setValue(f64, row_idx, val);
                            },
                            else => return error.UnsupportedType,
                        }
                    }
                }
                out_col.len = input_batch.row_count;
            }
            output_batch.row_count = input_batch.row_count;
        }

        return output_batch;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "projection: SELECT id, age" {
    const allocator = std.testing.allocator;

    // Create input batch with 3 columns: id, name_len, age
    const cols = [_]ColumnSchema{
        .{ .name = "id", .data_type = .int64, .nullable = false },
        .{ .name = "name_len", .data_type = .int32, .nullable = false },
        .{ .name = "age", .data_type = .int64, .nullable = false },
    };

    var schema = try Schema.init(allocator, &cols);
    defer schema.deinit();

    var builder = try batch_mod.BatchBuilder.init(allocator, &schema, 3);
    defer builder.deinit();

    // Row 0: id=1, name_len=5, age=25
    try builder.appendRow();
    builder.setValue(0, i64, 1);
    builder.setValue(1, i32, 5);
    builder.setValue(2, i64, 25);

    // Row 1: id=2, name_len=3, age=30
    try builder.appendRow();
    builder.setValue(0, i64, 2);
    builder.setValue(1, i32, 3);
    builder.setValue(2, i64, 30);

    // Row 2: id=3, name_len=7, age=35
    try builder.appendRow();
    builder.setValue(0, i64, 3);
    builder.setValue(1, i32, 7);
    builder.setValue(2, i64, 35);

    const input_batch = builder.finish();

    // Project: SELECT id, age (skip name_len)
    const project_cols = [_][]const u8{ "id", "age" };
    var projection = try Projection.init(allocator, &project_cols);
    defer projection.deinit();

    var output_batch = try projection.apply(&input_batch, null);
    defer output_batch.deinit();

    // Verify output has 2 columns
    try std.testing.expectEqual(@as(usize, 2), output_batch.columns.len);
    try std.testing.expectEqual(@as(usize, 3), output_batch.row_count);

    // Verify column 0 is id
    const id_col = output_batch.column(0);
    try std.testing.expectEqual(@as(?i64, 1), id_col.getValue(i64, 0));
    try std.testing.expectEqual(@as(?i64, 2), id_col.getValue(i64, 1));
    try std.testing.expectEqual(@as(?i64, 3), id_col.getValue(i64, 2));

    // Verify column 1 is age
    const age_col = output_batch.column(1);
    try std.testing.expectEqual(@as(?i64, 25), age_col.getValue(i64, 0));
    try std.testing.expectEqual(@as(?i64, 30), age_col.getValue(i64, 1));
    try std.testing.expectEqual(@as(?i64, 35), age_col.getValue(i64, 2));
}

test "projection with selection: SELECT id WHERE age > 25" {
    const allocator = std.testing.allocator;

    const cols = [_]ColumnSchema{
        .{ .name = "id", .data_type = .int64, .nullable = false },
        .{ .name = "age", .data_type = .int64, .nullable = false },
    };

    var schema = try Schema.init(allocator, &cols);
    defer schema.deinit();

    var builder = try batch_mod.BatchBuilder.init(allocator, &schema, 3);
    defer builder.deinit();

    // Row 0: id=1, age=20 (NOT selected)
    try builder.appendRow();
    builder.setValue(0, i64, 1);
    builder.setValue(1, i64, 20);

    // Row 1: id=2, age=30 (selected)
    try builder.appendRow();
    builder.setValue(0, i64, 2);
    builder.setValue(1, i64, 30);

    // Row 2: id=3, age=35 (selected)
    try builder.appendRow();
    builder.setValue(0, i64, 3);
    builder.setValue(1, i64, 35);

    const input_batch = builder.finish();

    // Create filter: age > 25
    const age_col_expr = expr_mod.column("age");
    const literal_25 = expr_mod.literalInt(25);
    const filter_expr = expr_mod.compare(.gt, &age_col_expr, &literal_25);

    var selection = try expr_mod.evaluateFilter(allocator, &filter_expr, &input_batch);
    defer selection.deinit();

    // Project: SELECT id
    const project_cols = [_][]const u8{"id"};
    var projection = try Projection.init(allocator, &project_cols);
    defer projection.deinit();

    var output_batch = try projection.apply(&input_batch, &selection);
    defer output_batch.deinit();

    // Should have 1 column, 2 rows
    try std.testing.expectEqual(@as(usize, 1), output_batch.columns.len);
    try std.testing.expectEqual(@as(usize, 2), output_batch.row_count);

    // Verify values are id=2, id=3
    const id_col = output_batch.column(0);
    try std.testing.expectEqual(@as(?i64, 2), id_col.getValue(i64, 0));
    try std.testing.expectEqual(@as(?i64, 3), id_col.getValue(i64, 1));
}
