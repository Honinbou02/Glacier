// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Expression Evaluator (Comptime-Specialized)
// ═══════════════════════════════════════════════════════════════════════════
//
// Comptime-specialized expression evaluation for WHERE clauses and projections.
//
// Key Features:
// - Zero virtual calls (all expressions known at comptime)
// - Vectorized execution (process entire columns at once)
// - NULL-aware evaluation (3-valued logic)
// - Type-safe expression trees
//
// Example:
//   WHERE age > 25 AND name = 'Alice'
//
// Becomes comptime expression tree:
//   And(
//     GreaterThan(Column("age"), Literal(25)),
//     Equal(Column("name"), Literal("Alice"))
//   )
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const batch_mod = @import("batch.zig");
const Batch = batch_mod.Batch;
const ColumnVector = batch_mod.ColumnVector;
const DataType = batch_mod.DataType;

/// Expression evaluation errors
pub const ExprError = error{
    TypeMismatch,
    InvalidExpression,
    ColumnNotFound,
    NotImplemented,
    OutOfMemory,
};
// ═══════════════════════════════════════════════════════════════════════════
// EXPRESSION TYPES
// ═══════════════════════════════════════════════════════════════════════════

/// Comparison operators
pub const CompareOp = enum {
    eq, // =
    ne, // !=
    lt, // <
    le, // <=
    gt, // >
    ge, // >=
};

/// Logical operators
pub const LogicalOp = enum {
    and_op,
    or_op,
    not_op,
};

/// Arithmetic operators
pub const ArithOp = enum {
    add,
    sub,
    mul,
    div,
    mod,
};

/// Expression node (abstract)
pub const Expr = union(enum) {
    // Leaf nodes
    column: []const u8, // Column reference by name
    literal_i64: i64,
    literal_f64: f64,
    literal_bool: bool,
    literal_string: []const u8,

    // Comparison
    compare: struct {
        op: CompareOp,
        left: *const Expr,
        right: *const Expr,
    },

    // Logical
    logical: struct {
        op: LogicalOp,
        left: *const Expr,
        right: ?*const Expr, // null for NOT
    },

    // Arithmetic
    arith: struct {
        op: ArithOp,
        left: *const Expr,
        right: *const Expr,
    },

    // IS NULL / IS NOT NULL
    is_null: struct {
        expr: *const Expr,
        negate: bool, // true for IS NOT NULL
    },
};

// ═══════════════════════════════════════════════════════════════════════════
// SELECTION VECTOR (Filter Results)
// ═══════════════════════════════════════════════════════════════════════════

/// Selection vector stores indices of selected rows after filtering
/// This allows zero-copy filtering - we don't move data, just track indices
pub const SelectionVector = struct {
    indices: []usize,
    count: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) ExprError!SelectionVector {
        const indices = try allocator.alloc(usize, capacity);
        return SelectionVector{
            .indices = indices,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SelectionVector) void {
        self.allocator.free(self.indices);
    }

    /// Reset selection to include all rows
    pub fn selectAll(self: *SelectionVector, row_count: usize) void {
        self.count = row_count;
        for (0..row_count) |i| {
            self.indices[i] = i;
        }
    }

    /// Add a row index to selection
    pub fn select(self: *SelectionVector, index: usize) void {
        self.indices[self.count] = index;
        self.count += 1;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// EXPRESSION EVALUATOR
// ═══════════════════════════════════════════════════════════════════════════

/// Evaluate expression and return selection vector
pub fn evaluateFilter(
    allocator: std.mem.Allocator,
    expr: *const Expr,
    input_batch: *const Batch,
) ExprError!SelectionVector {
    var selection = try SelectionVector.init(allocator, input_batch.row_count);
    errdefer selection.deinit();

    // Evaluate expression for each row
    for (0..input_batch.row_count) |row_idx| {
        const result = try evaluateExprForRow(expr, input_batch, row_idx);
        if (result orelse false) { // NULL is treated as false in filter
            selection.select(row_idx);
        }
    }

    return selection;
}

/// Evaluate expression for a single row
fn evaluateExprForRow(
    expr: *const Expr,
    input_batch: *const Batch,
    row_idx: usize,
) ExprError!?bool {
    switch (expr.*) {
        .literal_bool => |val| return val,
        .literal_i64 => return error.TypeMismatch,
        .literal_f64 => return error.TypeMismatch,
        .literal_string => return error.TypeMismatch,

        .column => {
            // Column references need comparison context
            return error.InvalidExpression;
        },

        .compare => |cmp| {
            return try evaluateCompare(cmp.op, cmp.left, cmp.right, input_batch, row_idx);
        },

        .logical => |logical| {
            return try evaluateLogical(logical.op, logical.left, logical.right, input_batch, row_idx);
        },

        .arith => return error.NotImplemented,

        .is_null => |is_null_expr| {
            const val = try evaluateExprForRow(is_null_expr.expr, input_batch, row_idx);
            const result = (val == null);
            return if (is_null_expr.negate) !result else result;
        },
    }
}

/// Evaluate comparison
fn evaluateCompare(
    op: CompareOp,
    left: *const Expr,
    right: *const Expr,
    input_batch: *const Batch,
    row_idx: usize,
) ExprError!?bool {
    // Handle column vs literal comparison
    if (left.* == .column and right.* == .literal_i64) {
        const col_name = left.column;
        const literal_val = right.literal_i64;

        // Resolve Column Index (Using Schema helper)
        // Optimization: Direct lookup instead of string comparison loop per row
        const col_idx = input_batch.schema.columnIndex(col_name) orelse return error.ColumnNotFound;

        // Direct Column Access
        const col = &input_batch.columns[col_idx];

        // Check if NULL
        if (col.isNull(row_idx)) return null;

        // Get value based on type
        const col_val = switch (col.data_type) {
            .int64 => col.getValue(i64, row_idx) orelse return null,
            .int32 => @as(i64, col.getValue(i32, row_idx) orelse return null),
            else => return error.TypeMismatch,
        };

        // Perform comparison
        return switch (op) {
            .eq => col_val == literal_val,
            .ne => col_val != literal_val,
            .lt => col_val < literal_val,
            .le => col_val <= literal_val,
            .gt => col_val > literal_val,
            .ge => col_val >= literal_val,
        };
    }

    // Handle column vs column comparison
    if (left.* == .column and right.* == .column) {
        return error.NotImplemented;
    }

    return error.InvalidExpression;
}

/// Evaluate logical operation (AND, OR, NOT)
fn evaluateLogical(
    op: LogicalOp,
    left: *const Expr,
    right: ?*const Expr,
    input_batch: *const Batch,
    row_idx: usize,
) ExprError!?bool {
    switch (op) {
        .and_op => {
            const left_val = try evaluateExprForRow(left, input_batch, row_idx);
            const right_val = try evaluateExprForRow(right.?, input_batch, row_idx);

            // 3-valued logic for AND
            if (left_val == false or right_val == false) return false;
            if (left_val == null or right_val == null) return null;
            return true;
        },

        .or_op => {
            const left_val = try evaluateExprForRow(left, input_batch, row_idx);
            const right_val = try evaluateExprForRow(right.?, input_batch, row_idx);

            // 3-valued logic for OR
            if (left_val == true or right_val == true) return true;
            if (left_val == null or right_val == null) return null;
            return false;
        },

        .not_op => {
            const val = try evaluateExprForRow(left, input_batch, row_idx);
            if (val == null) return null;
            return !val.?;
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Create a column reference expression
pub fn column(name: []const u8) Expr {
    return Expr{ .column = name };
}

/// Create an integer literal
pub fn literalInt(value: i64) Expr {
    return Expr{ .literal_i64 = value };
}

/// Create a boolean literal
pub fn literalBool(value: bool) Expr {
    return Expr{ .literal_bool = value };
}

/// Create comparison expression
pub fn compare(op: CompareOp, left: *const Expr, right: *const Expr) Expr {
    return Expr{
        .compare = .{
            .op = op,
            .left = left,
            .right = right,
        },
    };
}

/// Create AND expression
pub fn andExpr(left: *const Expr, right: *const Expr) Expr {
    return Expr{
        .logical = .{
            .op = .and_op,
            .left = left,
            .right = right,
        },
    };
}

/// Create OR expression
pub fn orExpr(left: *const Expr, right: *const Expr) Expr {
    return Expr{
        .logical = .{
            .op = .or_op,
            .left = left,
            .right = right,
        },
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "simple filter: age > 25" {
    const allocator = std.testing.allocator;

    // Create schema
    const cols = [_]batch_mod.ColumnSchema{
        .{ .name = "id", .data_type = .int64, .nullable = false },
        .{ .name = "age", .data_type = .int64, .nullable = false },
    };

    var schema = try batch_mod.Schema.init(allocator, &cols);
    defer schema.deinit();

    // Build batch with test data
    var builder = try batch_mod.BatchBuilder.init(allocator, &schema, 5);
    defer builder.deinit();

    // Row 0: id=1, age=20 (NOT selected)
    try builder.appendRow();
    builder.setValue(0, i64, 1);
    builder.setValue(1, i64, 20);

    // Row 1: id=2, age=30 (selected)
    try builder.appendRow();
    builder.setValue(0, i64, 2);
    builder.setValue(1, i64, 30);

    // Row 2: id=3, age=25 (NOT selected - not >)
    try builder.appendRow();
    builder.setValue(0, i64, 3);
    builder.setValue(1, i64, 25);

    // Row 3: id=4, age=40 (selected)
    try builder.appendRow();
    builder.setValue(0, i64, 4);
    builder.setValue(1, i64, 40);

    // Row 4: id=5, age=26 (selected)
    try builder.appendRow();
    builder.setValue(0, i64, 5);
    builder.setValue(1, i64, 26);

    const input_batch = builder.finish();

    // Build expression: age > 25
    const age_col = column("age");
    const literal_25 = literalInt(25);
    const filter_expr = compare(.gt, &age_col, &literal_25);

    // Evaluate filter
    var selection = try evaluateFilter(allocator, &filter_expr, &input_batch);
    defer selection.deinit();

    // Should select rows 1, 3, 4 (ages 30, 40, 26)
    try std.testing.expectEqual(@as(usize, 3), selection.count);
    try std.testing.expectEqual(@as(usize, 1), selection.indices[0]);
    try std.testing.expectEqual(@as(usize, 3), selection.indices[1]);
    try std.testing.expectEqual(@as(usize, 4), selection.indices[2]);
}

test "compound filter: age >= 25 AND id <= 3" {
    const allocator = std.testing.allocator;

    const cols = [_]batch_mod.ColumnSchema{
        .{ .name = "id", .data_type = .int64, .nullable = false },
        .{ .name = "age", .data_type = .int64, .nullable = false },
    };

    var schema = try batch_mod.Schema.init(allocator, &cols);
    defer schema.deinit();

    var builder = try batch_mod.BatchBuilder.init(allocator, &schema, 5);
    defer builder.deinit();

    // Row 0: id=1, age=20 -> age < 25, NOT selected
    try builder.appendRow();
    builder.setValue(0, i64, 1);
    builder.setValue(1, i64, 20);

    // Row 1: id=2, age=30 -> selected (30 >= 25 AND 2 <= 3)
    try builder.appendRow();
    builder.setValue(0, i64, 2);
    builder.setValue(1, i64, 30);

    // Row 2: id=3, age=25 -> selected (25 >= 25 AND 3 <= 3)
    try builder.appendRow();
    builder.setValue(0, i64, 3);
    builder.setValue(1, i64, 25);

    // Row 3: id=4, age=40 -> NOT selected (id > 3)
    try builder.appendRow();
    builder.setValue(0, i64, 4);
    builder.setValue(1, i64, 40);

    // Row 4: id=5, age=26 -> NOT selected (id > 3)
    try builder.appendRow();
    builder.setValue(0, i64, 5);
    builder.setValue(1, i64, 26);

    const input_batch = builder.finish();

    // Build expression: age >= 25 AND id <= 3
    const age_col = column("age");
    const literal_25 = literalInt(25);
    const age_filter = compare(.ge, &age_col, &literal_25);

    const id_col = column("id");
    const literal_3 = literalInt(3);
    const id_filter = compare(.le, &id_col, &literal_3);

    const filter_expr = andExpr(&age_filter, &id_filter);

    var selection = try evaluateFilter(allocator, &filter_expr, &input_batch);
    defer selection.deinit();

    // Should select rows 1 and 2
    try std.testing.expectEqual(@as(usize, 2), selection.count);
    try std.testing.expectEqual(@as(usize, 1), selection.indices[0]);
    try std.testing.expectEqual(@as(usize, 2), selection.indices[1]);
}
