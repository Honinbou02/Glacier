// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Physical Execution Engine
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const batch_mod = @import("batch.zig");
const planner_mod = @import("planner.zig");
const expr_mod = @import("expr.zig");
const parquet = @import("../formats/parquet.zig");
const compression = @import("../formats/compression.zig");
const value_decoder = @import("../formats/value_decoder.zig");

// ═══════════════════════════════════════════════════════════════════════════
// INTERFACES
// ═══════════════════════════════════════════════════════════════════════════

pub const BatchIterator = struct {
    ptr: *anyopaque,
    nextFn: *const fn (ctx: *anyopaque) anyerror!?batch_mod.Batch,
    deinitFn: *const fn (ctx: *anyopaque) void,

    pub fn next(self: BatchIterator) !?batch_mod.Batch {
        return self.nextFn(self.ptr);
    }

    pub fn deinit(self: BatchIterator) void {
        self.deinitFn(self.ptr);
    }
};

pub const ExecutionPlan = struct {
    ptr: *anyopaque,
    executeFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) anyerror!BatchIterator,
    schemaFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) anyerror!batch_mod.Schema,
    deinitFn: *const fn (ctx: *anyopaque) void,

    pub fn execute(self: ExecutionPlan, allocator: std.mem.Allocator) !BatchIterator {
        return self.executeFn(self.ptr, allocator);
    }

    pub fn schema(self: ExecutionPlan, allocator: std.mem.Allocator) !batch_mod.Schema {
        return self.schemaFn(self.ptr, allocator);
    }

    pub fn deinit(self: ExecutionPlan) void {
        self.deinitFn(self.ptr);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════════════════════

// --- ScanExec ---

pub const ScanExec = struct {
    table_path: []const u8,
    allocator: std.mem.Allocator,

    pub fn create(allocator: std.mem.Allocator, node: *const planner_mod.ScanNode) !ExecutionPlan {
        const self = try allocator.create(ScanExec);
        self.* = .{
            .table_path = try allocator.dupe(u8, node.table_name),
            .allocator = allocator,
        };

        return ExecutionPlan{
            .ptr = self,
            .executeFn = execute,
            .schemaFn = getSchema,
            .deinitFn = deinit,
        };
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *ScanExec = @ptrCast(@alignCast(ctx));
        self.allocator.free(self.table_path);
        self.allocator.destroy(self);
    }

    fn getSchema(ctx: *anyopaque, allocator: std.mem.Allocator) !batch_mod.Schema {
        const self: *ScanExec = @ptrCast(@alignCast(ctx));
        
        var reader = try parquet.Reader.open(allocator, self.table_path);
        defer reader.close();
        try reader.readMetadata();
        
        const metadata = reader.metadata.?;
        // Fix: Use ArrayListUnmanaged to avoid resolving issues
        var cols: std.ArrayListUnmanaged(batch_mod.ColumnSchema) = .{};
        defer cols.deinit(allocator);

        // Simple schema mapping based on flattened Parquet schema
        // Skipping root (index 0) if it is a struct wrapper (common in Parquet)
        var start_idx: usize = 0;
        if (metadata.schema.len > 0 and metadata.schema[0].num_children > 0) {
            start_idx = 1;
        }

        if (metadata.schema.len > start_idx) {
            for (metadata.schema[start_idx..]) |elem| {
                
                const dt = if (elem.type) |t| switch (t) {
                    .INT32 => batch_mod.DataType.int32,
                    .INT64 => batch_mod.DataType.int64,
                    .FLOAT => batch_mod.DataType.float32,
                    .DOUBLE => batch_mod.DataType.float64,
                    .BOOLEAN => batch_mod.DataType.boolean,
                    .BYTE_ARRAY => batch_mod.DataType.string,
                    else => batch_mod.DataType.string // Fallback
                } else batch_mod.DataType.string;

                try cols.append(allocator, .{
                    .name = try allocator.dupe(u8, elem.name),
                    .data_type = dt,
                    .nullable = (elem.repetition_type == .OPTIONAL),
                });
            }
        }
        
        return batch_mod.Schema.init(allocator, cols.items);
    }

    fn execute(ctx: *anyopaque, allocator: std.mem.Allocator) !BatchIterator {
        const self: *ScanExec = @ptrCast(@alignCast(ctx));
        
        // Re-read schema for the iterator to use
        var schema = try getSchema(ctx, allocator);
        errdefer schema.deinit(); 
        
        var reader = try parquet.Reader.open(allocator, self.table_path);
        errdefer reader.close();

        try reader.readMetadata();

        // Initialize dictionaries array (one per column)
        const dicts = try allocator.alloc(?batch_mod.Batch, schema.columns.len);
        for (dicts) |*d| d.* = null;

        const iter = try allocator.create(ScanIterator);
        iter.* = .{
            .allocator = allocator,
            .reader = reader,
            .current_row_group = 0,
            .schema = schema,
            .dictionaries = dicts,
        };

        return BatchIterator{
            .ptr = iter,
            .nextFn = ScanIterator.next,
            .deinitFn = ScanIterator.deinit,
        };
    }
};

const ScanIterator = struct {
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    current_row_group: usize,
    schema: batch_mod.Schema,
    dictionaries: []?batch_mod.Batch,

    fn next(ctx: *anyopaque) !?batch_mod.Batch {
        const self: *ScanIterator = @ptrCast(@alignCast(ctx));
        
        if (self.reader.metadata == null) return null;

        const row_groups = self.reader.metadata.?.row_groups;
        if (self.current_row_group >= row_groups.len) {
            return null; // EOF
        }

        const rg = row_groups[self.current_row_group];
        self.current_row_group += 1;

        // Create batch
        var num_rows = @as(usize, @intCast(rg.num_rows));
        
        // Fallback: If RowGroup has 0 rows but file has rows and it's the only RG, assume it contains all rows.
        if (num_rows == 0 and row_groups.len == 1) {
            num_rows = @as(usize, @intCast(self.reader.metadata.?.num_rows));
            std.debug.print("[Scan] Warning: RowGroup.num_rows is 0, using FileMetaData.num_rows = {d}\n", .{num_rows});
        }

        if (num_rows == 0) return try next(ctx); // Skip empty row groups

        var builder = try batch_mod.BatchBuilder.init(self.allocator, &self.schema, num_rows);
        errdefer builder.deinit();
        
        // DATA READING LOGIC
        for (rg.columns, 0..) |col_chunk, i| {
             if (i >= self.schema.columns.len) break;
             const col_schema = self.schema.columns[i];
             
             // 1. Load Dictionary if available and not yet loaded
             if (col_chunk.meta_data.dictionary_page_offset) |dict_offset| {
                 if (self.dictionaries[i] == null and dict_offset > 0) {
                     // Read Dictionary Page using if/else capture instead of catch block
                     if (self.reader.readDataPage(dict_offset, self.allocator)) |page_res| {
                         defer self.allocator.free(page_res.compressed_data);
                         
                         const uncompressed_size = @as(usize, @intCast(page_res.page_header.uncompressed_page_size));
                         const page_data = try compression.decompress(self.allocator, col_chunk.meta_data.codec, page_res.compressed_data, uncompressed_size);
                         defer self.allocator.free(page_data);
                         const num_dict_values = if (page_res.page_header.dictionary_page_header) |h| h.num_values else if (page_res.page_header.data_page_header) |h| h.num_values else 0;
                         
                         var dict_batch = try batch_mod.Batch.initWithSchema(self.allocator, &self.schema, @intCast(num_dict_values));
                         var dict_col = dict_batch.columnMut(i);
                         dict_col.len = @intCast(num_dict_values);

                         // Decode Dictionary Values (ALWAYS PLAIN)
                         if (col_schema.data_type == .string) {
                             const vals = try value_decoder.decodePlainByteArray(self.allocator, page_data, @intCast(num_dict_values), false);
                             const slice_ptr = @as([*][]u8, @ptrCast(@alignCast(dict_col.data.ptr)));
                             for (vals, 0..) |v, r| slice_ptr[r] = v;
                             self.allocator.free(vals);
                         } else if (col_schema.data_type == .int64) {
                              const vals = try value_decoder.decodePlainInt64(self.allocator, page_data, @intCast(num_dict_values), false);
                              defer self.allocator.free(vals);
                              for (vals, 0..) |v, r| dict_col.setValue(i64, r, v);
                         } else if (col_schema.data_type == .int32) {
                              const vals = try value_decoder.decodePlainInt32(self.allocator, page_data, @intCast(num_dict_values), false);
                              defer self.allocator.free(vals);
                              for (vals, 0..) |v, r| dict_col.setValue(i32, r, v);
                         }
                         
                         self.dictionaries[i] = dict_batch;
                     } else |err| {
                         std.debug.print("[Scan] Dictionary Load Error col {d}: {s}\n", .{i, @errorName(err)});
                         // Proceed without dictionary? will likely fail later if needed.
                     }
                 }
             }

             // 2. Read Data Page
             if (self.reader.readDataPage(col_chunk.meta_data.data_page_offset, self.allocator)) |page_res| {
                 defer self.allocator.free(page_res.compressed_data);
                 
                 const uncompressed_size = @as(usize, @intCast(page_res.page_header.uncompressed_page_size));
                 const page_data = try compression.decompress(self.allocator, col_chunk.meta_data.codec, page_res.compressed_data, uncompressed_size);
                 defer self.allocator.free(page_data);
                 
                 const encoding = if (page_res.page_header.data_page_header) |h| h.encoding else .PLAIN;

                 var batch_col = builder.batch.columnMut(i);
                 batch_col.len = num_rows;
                 
                 const using_dictionary = (encoding == .PLAIN_DICTIONARY or encoding == .RLE_DICTIONARY);
                 
                 if (using_dictionary) {
                     if (self.dictionaries[i] == null) {
                         std.debug.print("[Scan] Error: Dict encoding without Dict loaded!\n", .{});
                         continue;
                     }
                     const dict = self.dictionaries[i].?;
                     const dict_col = dict.column(i);

                     const indices = value_decoder.decodeDictionaryIndices(self.allocator, page_data, num_rows) catch |err| {
                         std.debug.print("[Scan] Indices Error: {s}\n", .{@errorName(err)});
                         continue;
                     };
                     defer self.allocator.free(indices);

                     for (indices, 0..) |idx, r| {
                         if (r >= num_rows) break;
                         const idx_usize = @as(usize, @intCast(idx));
                         
                         if (col_schema.data_type == .string) {
                             const val = dict_col.getValue([]u8, idx_usize).?; 
                             const slice_ptr = @as([*][]u8, @ptrCast(@alignCast(batch_col.data.ptr)));
                             slice_ptr[r] = val; 
                         } else if (col_schema.data_type == .int64) {
                             const val = dict_col.getValue(i64, idx_usize).?;
                             batch_col.setValue(i64, r, val);
                         } else if (col_schema.data_type == .int32) {
                             const val = dict_col.getValue(i32, idx_usize).?;
                             batch_col.setValue(i32, r, val);
                         } else if (col_schema.data_type == .float32) {
                             const val = dict_col.getValue(f32, idx_usize).?;
                             batch_col.setValue(f32, r, val);
                         } else if (col_schema.data_type == .float64) {
                             const val = dict_col.getValue(f64, idx_usize).?;
                             batch_col.setValue(f64, r, val);
                         }
                         batch_col.setNull(r, false);
                     }
                 } else {
                     // PLAIN Encoding (but might have RLE header for def/rep levels)
                     if (col_schema.data_type == .int64) {
                         // Check if data starts with RLE/hybrid header
                         // Format: length(4 bytes) + bit_width(1 byte) + ...
                         // Common pattern: small varints at start
                         var decode_offset: usize = 0;
                         
                         // Heuristic: if first 4 bytes are small (< 100) it might be a header
                         // Skip potential RLE header (up to 6 bytes based on our observation)
                         if (page_data.len >= 6) {
                             const potential_header = std.mem.readInt(u32, page_data[0..4], .little);
                             if (potential_header < 256) {
                                 decode_offset = 6;
                             }
                         }
                         
                         const actual_data = page_data[decode_offset..];
                         const vals = value_decoder.decodePlainInt64(self.allocator, actual_data, num_rows, false) catch continue;
                         defer self.allocator.free(vals);
                         for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(i64, r, v);
                     } else if (col_schema.data_type == .int32) {
                         const vals = value_decoder.decodePlainInt32(self.allocator, page_data, num_rows, false) catch continue;
                         defer self.allocator.free(vals);
                         for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(i32, r, v);
                     } else if (col_schema.data_type == .float32) {
                         // Same RLE header detection as INT64
                         var decode_offset: usize = 0;
                         if (page_data.len >= 6) {
                             const potential_header = std.mem.readInt(u32, page_data[0..4], .little);
                             if (potential_header < 256) {
                                 decode_offset = 6;
                             }
                         }
                         const actual_data = page_data[decode_offset..];
                         const vals = value_decoder.decodePlainFloat32(self.allocator, actual_data, num_rows, false) catch continue;
                         defer self.allocator.free(vals);
                         for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(f32, r, v);
                     } else if (col_schema.data_type == .float64) {
                         // Same RLE header detection as INT64
                         var decode_offset: usize = 0;
                         if (page_data.len >= 6) {
                             const potential_header = std.mem.readInt(u32, page_data[0..4], .little);
                             if (potential_header < 256) {
                                 decode_offset = 6;
                             }
                         }
                         const actual_data = page_data[decode_offset..];
                         const vals = value_decoder.decodePlainFloat64(self.allocator, actual_data, num_rows, false) catch continue;
                         defer self.allocator.free(vals);
                         for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(f64, r, v);
                     } else if (col_schema.data_type == .string) {
                         const vals = value_decoder.decodePlainByteArray(self.allocator, page_data, num_rows, false) catch continue;
                         const slice_ptr = @as([*][]u8, @ptrCast(@alignCast(batch_col.data.ptr)));
                         for (vals, 0..) |v, r| {
                             if (r < num_rows) {
                                 slice_ptr[r] = v;
                                 batch_col.setNull(r, false);
                             }
                         }
                         
                         // Free the array of slices (not the string contents - Batch owns those via deep deinit)
                         self.allocator.free(vals); 
                     }
                 }

             } else |err| {
                 std.debug.print("[Scan] ReadPage Error col {d}: {s}\n", .{i, @errorName(err)});
                 continue;
             }
        }
        
        builder.batch.row_count = num_rows;
        return builder.finish();
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *ScanIterator = @ptrCast(@alignCast(ctx));
        
        for (self.schema.columns) |col| {
            self.allocator.free(col.name);
        }
        
        for (self.dictionaries) |*d| {
            if (d.*) |*batch| batch.deinit();
        }
        self.allocator.free(self.dictionaries);

        var mut_schema = self.schema;
        mut_schema.deinit();
        self.reader.close();
        self.allocator.destroy(self);
    }
};

// --- FilterExec ---

pub const FilterExec = struct {
    input: ExecutionPlan,
    condition: expr_mod.Expr,
    allocator: std.mem.Allocator,

    pub fn create(allocator: std.mem.Allocator, input: ExecutionPlan, condition: expr_mod.Expr) !ExecutionPlan {
        const self = try allocator.create(FilterExec);
        self.* = .{
            .input = input,
            .condition = condition,
            .allocator = allocator,
        };
        
        return ExecutionPlan{
             .ptr = self,
             .executeFn = execute,
             .schemaFn = getSchema,
             .deinitFn = deinit,
        };
    }
    
    fn deinit(ctx: *anyopaque) void {
        const self: *FilterExec = @ptrCast(@alignCast(ctx));
        self.input.deinit();
        self.allocator.destroy(self);
    }

    fn getSchema(ctx: *anyopaque, allocator: std.mem.Allocator) !batch_mod.Schema {
        const self: *FilterExec = @ptrCast(@alignCast(ctx));
        return self.input.schema(allocator);
    }

    fn execute(ctx: *anyopaque, allocator: std.mem.Allocator) !BatchIterator {
        const self: *FilterExec = @ptrCast(@alignCast(ctx));
        const input_iter = try self.input.execute(allocator);
        
        const iter = try allocator.create(FilterIterator);
        iter.* = .{
            .input_iter = input_iter,
            .condition = &self.condition, 
            .allocator = allocator,
        };

        return BatchIterator{
            .ptr = iter,
            .nextFn = FilterIterator.next,
            .deinitFn = FilterIterator.deinit,
        };
    }
};

const FilterIterator = struct {
    input_iter: BatchIterator,
    condition: *const expr_mod.Expr,
    allocator: std.mem.Allocator,

    fn next(ctx: *anyopaque) !?batch_mod.Batch {
        const self: *FilterIterator = @ptrCast(@alignCast(ctx));
        
        while (true) {
            const batch_opt = try self.input_iter.next();
            if (batch_opt) |batch| {
                // Apply filter
                var selection = try expr_mod.evaluateFilter(self.allocator, self.condition, &batch);
                defer selection.deinit();

                if (selection.count == 0) {
                    var mut_batch = batch;
                    mut_batch.deinit();
                    continue;
                }
                
                if (selection.count == batch.row_count) {
                    return batch; 
                }
                
                // For POC, return full batch if ANY pass
                return batch; 
            } else {
                return null;
            }
        }
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *FilterIterator = @ptrCast(@alignCast(ctx));
        self.input_iter.deinit();
        self.allocator.destroy(self);
    }
};
