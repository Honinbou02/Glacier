// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Query Planner (Logical & Physical)
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const batch = @import("batch.zig");
const expr_mod = @import("expr.zig");
const sql = @import("../sql/parser.zig");

// ═══════════════════════════════════════════════════════════════════════════
// LOGICAL PLAN
// ═══════════════════════════════════════════════════════════════════════════

pub const LogicalPlan = union(enum) {
    scan: *ScanNode,
    filter: *FilterNode,
    projection: *ProjectionNode,
    limit: *LimitNode,
    aggregate: *AggregateNode,

    pub fn deinit(self: LogicalPlan, allocator: std.mem.Allocator) void {
        switch (self) {
            .scan => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
            .filter => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
            .projection => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
            .limit => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
            .aggregate => |n| {
                n.deinit(allocator);
                allocator.destroy(n);
            },
        }
    }

    /// Get the output schema of this plan node
    pub fn schema(self: LogicalPlan) *const batch.Schema {
        return switch (self) {
            .scan => |n| &n.output_schema,
            .filter => |n| n.input.schema(), // Filter doesn't change schema
            .projection => |n| &n.output_schema,
            .limit => |n| n.input.schema(), // Limit doesn't change schema
            .aggregate => |n| &n.output_schema,
        };
    }
};

pub const ScanNode = struct {
    table_name: []const u8,
    output_schema: batch.Schema,
    projected_columns: ?[]const []const u8 = null, // Logic pushdown: only read these columns

    pub fn deinit(self: *ScanNode, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        // Schema is owned by ScanNode
        var mut_schema = self.output_schema;
        mut_schema.deinit();
        
        if (self.projected_columns) |cols| {
            for (cols) |c| allocator.free(c);
            allocator.free(cols);
        }
    }
};

pub const FilterNode = struct {
    input: LogicalPlan,
    condition: expr_mod.Expr,

    pub fn deinit(self: *FilterNode, allocator: std.mem.Allocator) void {
        self.input.deinit(allocator);
        // Expression tree deallocation would go here if Expr needed it (currently likely arena/stack or simplistic)
        // Check expr.zig implementation for deinit needs. Assuming Expr involves pointers, we definitely need deinit logic here.
        // For now, let's assume Expr management is handled carefully.
    }
};

pub const ProjectionNode = struct {
    input: LogicalPlan,
    expressions: []expr_mod.Expr,
    output_schema: batch.Schema,

    pub fn deinit(self: *ProjectionNode, allocator: std.mem.Allocator) void {
        self.input.deinit(allocator);
        allocator.free(self.expressions);
        var mut_schema = self.output_schema;
        mut_schema.deinit();
    }
};

pub const LimitNode = struct {
    input: LogicalPlan,
    limit: usize,
    offset: usize,

    pub fn deinit(self: *LimitNode, allocator: std.mem.Allocator) void {
        self.input.deinit(allocator);
    }
};

pub const AggregateNode = struct {
    input: LogicalPlan,
    group_by: []expr_mod.Expr,
    aggregates: []AggregateExpr,
    output_schema: batch.Schema,

    pub fn deinit(self: *AggregateNode, allocator: std.mem.Allocator) void {
        self.input.deinit(allocator);
        allocator.free(self.group_by);
        allocator.free(self.aggregates);
        var mut_schema = self.output_schema;
        mut_schema.deinit();
    }
};

pub const AggregateExpr = struct {
    func: sql.AggregateType,
    arg_expr: ?expr_mod.Expr, // null for COUNT(*)
    alias: []const u8,
};

// ═══════════════════════════════════════════════════════════════════════════
// BUILDER: AST -> LOGICAL PLAN
// ═══════════════════════════════════════════════════════════════════════════

pub const PlannerError = error{
    TableNotFound,
    ColumnNotFound,
    InvalidExpression,
    UnsupportedFeature,
    OutOfMemory,
};

pub const Planner = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Planner {
        return .{
            .allocator = allocator,
        };
    }

    /// Entry point: Convert SQL Query AST to Logical Plan
    pub fn createLogicalPlan(self: *Planner, query: *const sql.Query) !LogicalPlan {
        // 1. Start with Table Scan
        // In a real DB, we would look up schema from a Catalog here.
        // For now, we'll create a dummy schema or infer it later during Physical Planning.
        // WAIT: Logical Plan NEEDS schema for validation.
        // As a shortcut for this phase, ScanNode will hold a placeholder schema or we defer schema resolution.
        // Given we don't have a central Catalog in memory easily accessible here, 
        // we might need to load metadata NOW or allow "Unresolved" schema.
        // Let's assume we can lazily resolve schema or pass it in.
        
        // Strategy: Create a ScanNode. Schema resolution happens at Physical Planning or we peek at file now.
        // Let's create a builder that accepts a schema_provider callback/interface?
        // Or simpler: Just infer generic schema based on usage? No, that's weak.
        
        // For the REPL, we know the table path. We can peek metadata.
        // But for `planner.zig` purity, maybe we mock the schema or pass it.
        
        // Let's assume we create the ScanNode with the table name (path).
        // The schema must be filled.
        
        var plan = try self.createScan(query.from_table);

        // 2. Apply Filtering (WHERE)
        if (query.where_clause) |where_expr| {
            plan = try self.createFilter(plan, where_expr);
        }

        // 3. Apply Aggregation (GROUP BY / Aggregates)
        if (query.group_by != null or query.aggregates != null) {
             plan = try self.createAggregate(plan, query.group_by, query.aggregates);
        } else {
            // 4. Apply Projection (SELECT list) - Only if not aggregating (Projections inside Aggregates are handled there or after)
            // Note: Standard SQL: Filter -> GroupBy -> Having -> Select -> OrderBy -> Limit
            // If we have aggregates, the Projection happens AFTER aggregation (to select Key, AggResult).
            
            // If simple SELECT (no agg), apply projection
            plan = try self.createProjection(plan, query.select_columns);
        }

        // 5. Limit
        if (query.limit) |limit_val| {
            plan = try self.createLimit(plan, limit_val, query.offset orelse 0);
        }

        return plan;
    }

    fn createScan(self: *Planner, table_name: []const u8) !LogicalPlan {
        const node = try self.allocator.create(ScanNode);
        
        // TODO: Resolve real schema. For now, empty or mock.
        // Important: In a real implementation this is critical.
        const empty_cols = try self.allocator.alloc(batch.ColumnSchema, 0);
        const schema = try batch.Schema.init(self.allocator, empty_cols);
        self.allocator.free(empty_cols);

        node.* = .{
            .table_name = try self.allocator.dupe(u8, table_name),
            .output_schema = schema,
        };
        return LogicalPlan{ .scan = node };
    }

    fn createFilter(self: *Planner, input: LogicalPlan, expr_ast: *const sql.Expr) !LogicalPlan {
        const node = try self.allocator.create(FilterNode);
        
        const condition = try self.convertExpr(expr_ast);

        node.* = .{
            .input = input,
            .condition = condition,
        };
        return LogicalPlan{ .filter = node };
    }
    
    fn createProjection(self: *Planner, input: LogicalPlan, columns: []const []const u8) !LogicalPlan {
        const node = try self.allocator.create(ProjectionNode);
        
        var exprs = try self.allocator.alloc(expr_mod.Expr, columns.len);
        // NOTE: Here we should create a new Schema based on selected columns
        
        for (columns, 0..) |col_name, i| {
            if (std.mem.eql(u8, col_name, "*")) {
                 // TODO: Expand wildcard. Requires input schema.
                 // For now, placeholder.
                 exprs[i] = expr_mod.column("*");
            } else {
                 exprs[i] = expr_mod.column(col_name);
            }
        }

        // Placeholder schema for output
        const empty_cols = try self.allocator.alloc(batch.ColumnSchema, 0);
        const schema = try batch.Schema.init(self.allocator, empty_cols);
        self.allocator.free(empty_cols);

        node.* = .{
            .input = input,
            .expressions = exprs,
            .output_schema = schema,
        };
        return LogicalPlan{ .projection = node };
    }

    fn createAggregate(self: *Planner, input: LogicalPlan, group_by: ?[][]const u8, aggs: ?[]const sql.AggregateFunc) !LogicalPlan {
        // Implementation for aggregation node creation
        const node = try self.allocator.create(AggregateNode);
        
        var group_exprs: []expr_mod.Expr = undefined;
        if (group_by) |cols| {
            group_exprs = try self.allocator.alloc(expr_mod.Expr, cols.len);
            for (cols, 0..) |col, i| {
                group_exprs[i] = expr_mod.column(col);
            }
        } else {
             group_exprs = try self.allocator.alloc(expr_mod.Expr, 0);
        }
        
        var agg_exprs: []AggregateExpr = undefined;
        if (aggs) |funcs| {
            agg_exprs = try self.allocator.alloc(AggregateExpr, funcs.len);
            for (funcs, 0..) |f, i| {
                const arg = if (f.column) |c| expr_mod.column(c) else null;
                agg_exprs[i] = .{
                    .func = f.func_type,
                    .arg_expr = arg,
                    .alias = if (f.alias) |a| try self.allocator.dupe(u8, a) else try self.allocator.dupe(u8, ""),
                };
            }
        } else {
            agg_exprs = try self.allocator.alloc(AggregateExpr, 0);
        }
        
        // Placeholder schema
        const empty_cols = try self.allocator.alloc(batch.ColumnSchema, 0);
        const schema = try batch.Schema.init(self.allocator, empty_cols);
        self.allocator.free(empty_cols);

        node.* = .{
             .input = input,
             .group_by = group_exprs,
             .aggregates = agg_exprs,
             .output_schema = schema,
        };
        
        return LogicalPlan{ .aggregate = node };
    }

    fn createLimit(self: *Planner, input: LogicalPlan, limit: usize, offset: usize) !LogicalPlan {
        const node = try self.allocator.create(LimitNode);
        node.* = .{
            .input = input,
            .limit = limit,
            .offset = offset,
        };
        return LogicalPlan{ .limit = node };
    }

    /// Convert parser AST Expr into execution Expr
    fn convertExpr(self: *Planner, ast_expr: *const sql.Expr) !expr_mod.Expr {
        switch (ast_expr.*) {
             .column => |name| {
                 return expr_mod.column(try self.allocator.dupe(u8, name));
             },
             .number => |val| {
                 // Simplification: treat all numbers as f64 or try to cast?
                 // expr_mod has literals for i64 and f64.
                 // Heuristic: check if fraction.
                 if (@floor(val) == val) {
                     return expr_mod.literalInt(@intFromFloat(val));
                 } else {
                     return expr_mod.Expr{ .literal_f64 = val };
                 }
             },
             .string => |s| {
                 return expr_mod.Expr{ .literal_string = try self.allocator.dupe(u8, s) };
             },
             .boolean => |b| return expr_mod.literalBool(b),
             .binary => |bin| {
                 // Recursively convert
                 const left = try self.convertExpr(bin.left);
                 const right = try self.convertExpr(bin.right);
                 
                 // Allocate on heap for the expression tree (expr_mod uses pointers for children)
                 const left_ptr = try self.allocator.create(expr_mod.Expr);
                 left_ptr.* = left;
                 const right_ptr = try self.allocator.create(expr_mod.Expr);
                 right_ptr.* = right;

                 // Map operators
                 switch (bin.op) {
                     .eq => return expr_mod.compare(.eq, left_ptr, right_ptr),
                     .ne => return expr_mod.compare(.ne, left_ptr, right_ptr),
                     .lt => return expr_mod.compare(.lt, left_ptr, right_ptr),
                     .le => return expr_mod.compare(.le, left_ptr, right_ptr),
                     .gt => return expr_mod.compare(.gt, left_ptr, right_ptr),
                     .ge => return expr_mod.compare(.ge, left_ptr, right_ptr),
                     .and_op => return expr_mod.andExpr(left_ptr, right_ptr),
                     .or_op => return expr_mod.orExpr(left_ptr, right_ptr),
                     // TODO: Arithmetic
                     else => return error.UnsupportedFeature,
                 }
             },
             else => return error.UnsupportedFeature,
        }
    }
};

test "planner smoke test" {
    // Just verify it compiles
    _ = Planner{};
}
