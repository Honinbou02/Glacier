// ═══════════════════════════════════════════════════════════════════════════
// APACHE ICEBERG TABLE FORMAT (V2)
// ═══════════════════════════════════════════════════════════════════════════
//
// Iceberg is a table format for huge analytic datasets.
//
// DIRECTORY STRUCTURE:
//   /warehouse/my_table/
//     metadata/
//       v1.metadata.json       <- TableMetadata
//       v2.metadata.json
//       snap-123.avro          <- Snapshot manifest
//       manifest-abc.avro      <- Data file manifest
//     data/
//       00000-0-data.parquet   <- Actual data files
//       00001-1-data.parquet
//
// REFERENCES:
//   - https://iceberg.apache.org/spec/
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const errors = @import("../core/errors.zig");

// ═══════════════════════════════════════════════════════════════════════════
// SCHEMA TYPES
// ═══════════════════════════════════════════════════════════════════════════

/// Iceberg primitive types
pub const PrimitiveType = enum {
    boolean,
    int,
    long,
    float,
    double,
    decimal,
    date,
    time,
    timestamp,
    timestamptz,
    string,
    uuid,
    fixed,
    binary,
};

/// Schema field
pub const SchemaField = struct {
    id: i32,
    name: []const u8,
    required: bool,
    type_name: []const u8,
    doc: ?[]const u8 = null,
};

/// Table schema
pub const Schema = struct {
    schema_id: i32,
    fields: []SchemaField,

    allocator: std.mem.Allocator,

    pub fn deinit(self: *Schema) void {
        // Free each field's strings
        for (self.fields) |*field| {
            self.allocator.free(field.name);
            self.allocator.free(field.type_name);
            if (field.doc) |doc| {
                self.allocator.free(doc);
            }
        }
        self.allocator.free(self.fields);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT METADATA
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot summary
pub const SnapshotSummary = struct {
    operation: []const u8,
    added_data_files: ?i64 = null,
    deleted_data_files: ?i64 = null,
    added_records: ?i64 = null,
    deleted_records: ?i64 = null,
};

/// Snapshot
pub const Snapshot = struct {
    snapshot_id: i64,
    parent_snapshot_id: ?i64,
    sequence_number: i64,
    timestamp_ms: i64,
    manifest_list: []const u8,
    summary: ?SnapshotSummary,
    schema_id: ?i32,
};

// ═══════════════════════════════════════════════════════════════════════════
// PARTITION SPEC
// ═══════════════════════════════════════════════════════════════════════════

/// Partition field transform
pub const Transform = enum {
    identity,
    bucket,
    truncate,
    year,
    month,
    day,
    hour,
};

/// Partition field
pub const PartitionField = struct {
    field_id: i32,
    source_id: i32,
    name: []const u8,
    transform: []const u8,
};

/// Partition spec
pub const PartitionSpec = struct {
    spec_id: i32,
    fields: []PartitionField,

    allocator: std.mem.Allocator,

    pub fn deinit(self: *PartitionSpec) void {
        // Free each field's strings
        for (self.fields) |*field| {
            self.allocator.free(field.name);
            self.allocator.free(field.transform);
        }
        self.allocator.free(self.fields);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TABLE METADATA (metadata.json)
// ═══════════════════════════════════════════════════════════════════════════

/// Complete Iceberg table metadata
pub const TableMetadata = struct {
    format_version: i32,
    table_uuid: []const u8,
    location: []const u8,
    last_updated_ms: i64,
    last_column_id: i32,

    // Current state
    current_snapshot_id: ?i64,
    current_schema_id: i32,
    default_spec_id: i32,

    // Collections
    schemas: []Schema,
    partition_specs: []PartitionSpec,
    snapshots: []Snapshot,

    allocator: std.mem.Allocator,

    pub fn deinit(self: *TableMetadata) void {
        // Free strings
        self.allocator.free(self.table_uuid);
        self.allocator.free(self.location);

        // Free schemas
        for (self.schemas) |*schema| {
            schema.deinit();
        }
        self.allocator.free(self.schemas);

        // Free partition specs
        for (self.partition_specs) |*spec| {
            spec.deinit();
        }
        self.allocator.free(self.partition_specs);

        // Free snapshots
        for (self.snapshots) |*snap| {
            self.allocator.free(snap.manifest_list);
            if (snap.summary) |*sum| {
                self.allocator.free(sum.operation);
            }
        }
        self.allocator.free(self.snapshots);
    }

    /// Get the current snapshot (if any)
    pub fn getCurrentSnapshot(self: *const TableMetadata) ?*const Snapshot {
        if (self.current_snapshot_id == null) {
            return null;
        }

        const target_id = self.current_snapshot_id.?;

        for (self.snapshots) |*snap| {
            if (snap.snapshot_id == target_id) {
                return snap;
            }
        }

        return null;
    }

    /// Get schema by ID
    pub fn getSchema(self: *const TableMetadata, schema_id: i32) ?*const Schema {
        for (self.schemas) |*schema| {
            if (schema.schema_id == schema_id) {
                return schema;
            }
        }
        return null;
    }

    /// Get current schema
    pub fn getCurrentSchema(self: *const TableMetadata) ?*const Schema {
        return self.getSchema(self.current_schema_id);
    }

    /// Parse TableMetadata from metadata.json
    pub fn parseFromJson(allocator: std.mem.Allocator, json_data: []const u8) !TableMetadata {
        const parsed = try std.json.parseFromSlice(
            std.json.Value,
            allocator,
            json_data,
            .{ .allocate = .alloc_always },
        );
        defer parsed.deinit();

        const root = parsed.value.object;

        // Parse basic fields (duplicate strings to own them)
        const format_version = @as(i32, @intCast(root.get("format-version").?.integer));
        const table_uuid = try allocator.dupe(u8, root.get("table-uuid").?.string);
        errdefer allocator.free(table_uuid);
        const location = try allocator.dupe(u8, root.get("location").?.string);
        errdefer allocator.free(location);
        const last_updated_ms = root.get("last-updated-ms").?.integer;
        const last_column_id = @as(i32, @intCast(root.get("last-column-id").?.integer));

        const current_snapshot_id = if (root.get("current-snapshot-id")) |val|
            val.integer
        else
            null;

        const current_schema_id = @as(i32, @intCast(root.get("current-schema-id").?.integer));
        const default_spec_id = @as(i32, @intCast(root.get("default-spec-id").?.integer));

        // Parse schemas
        const schemas_json = root.get("schemas").?.array;
        var schemas = try allocator.alloc(Schema, schemas_json.items.len);
        errdefer allocator.free(schemas);

        for (schemas_json.items, 0..) |schema_json, i| {
            schemas[i] = try parseSchema(allocator, schema_json.object);
        }
        errdefer {
            for (schemas[0..schemas.len]) |*schema| {
                schema.deinit();
            }
        }

        // Parse partition specs (support both "partition-spec" and "partition-specs")
        const specs_json = if (root.get("partition-specs")) |specs|
            specs.array
        else if (root.get("partition-spec")) |spec|
            spec.array
        else
            return error.MissingPartitionSpec;
        var partition_specs = try allocator.alloc(PartitionSpec, specs_json.items.len);
        errdefer allocator.free(partition_specs);

        for (specs_json.items, 0..) |spec_json, i| {
            partition_specs[i] = try parsePartitionSpec(allocator, spec_json.object);
        }
        errdefer {
            for (partition_specs[0..partition_specs.len]) |*spec| {
                spec.deinit();
            }
        }

        // Parse snapshots (optional)
        var snapshots: []Snapshot = &.{};
        if (root.get("snapshots")) |snapshots_json| {
            snapshots = try allocator.alloc(Snapshot, snapshots_json.array.items.len);
            errdefer allocator.free(snapshots);

            for (snapshots_json.array.items, 0..) |snap_json, i| {
                snapshots[i] = try parseSnapshot(allocator, snap_json.object);
            }
            errdefer {
                for (snapshots[0..snapshots.len]) |snap| {
                    allocator.free(snap.manifest_list);
                    if (snap.summary) |sum| {
                        allocator.free(sum.operation);
                    }
                }
            }
        }

        return TableMetadata{
            .format_version = format_version,
            .table_uuid = table_uuid,
            .location = location,
            .last_updated_ms = last_updated_ms,
            .last_column_id = last_column_id,
            .current_snapshot_id = current_snapshot_id,
            .current_schema_id = current_schema_id,
            .default_spec_id = default_spec_id,
            .schemas = schemas,
            .partition_specs = partition_specs,
            .snapshots = snapshots,
            .allocator = allocator,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// JSON PARSING HELPERS
// ═══════════════════════════════════════════════════════════════════════════

fn parseSchema(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !Schema {
    const schema_id = @as(i32, @intCast(obj.get("schema-id").?.integer));
    const fields_json = obj.get("fields").?.array;

    var fields = try allocator.alloc(SchemaField, fields_json.items.len);
    errdefer allocator.free(fields);

    for (fields_json.items, 0..) |field_json, i| {
        fields[i] = try parseSchemaField(allocator, field_json.object);
    }
    errdefer {
        for (fields[0..fields.len]) |*field| {
            allocator.free(field.name);
            allocator.free(field.type_name);
            if (field.doc) |doc| {
                allocator.free(doc);
            }
        }
    }

    return Schema{
        .schema_id = schema_id,
        .fields = fields,
        .allocator = allocator,
    };
}

fn parseSchemaField(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !SchemaField {
    const id = @as(i32, @intCast(obj.get("id").?.integer));
    const name = try allocator.dupe(u8, obj.get("name").?.string);
    errdefer allocator.free(name);
    const required = obj.get("required").?.bool;
    const type_name = try allocator.dupe(u8, obj.get("type").?.string);
    errdefer allocator.free(type_name);
    const doc = if (obj.get("doc")) |d| try allocator.dupe(u8, d.string) else null;
    errdefer if (doc) |d| allocator.free(d);

    return SchemaField{
        .id = id,
        .name = name,
        .required = required,
        .type_name = type_name,
        .doc = doc,
    };
}

fn parsePartitionSpec(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !PartitionSpec {
    const spec_id = @as(i32, @intCast(obj.get("spec-id").?.integer));
    const fields_json = obj.get("fields").?.array;

    var fields = try allocator.alloc(PartitionField, fields_json.items.len);
    errdefer allocator.free(fields);

    for (fields_json.items, 0..) |field_json, i| {
        fields[i] = try parsePartitionField(allocator, field_json.object);
    }
    errdefer {
        for (fields[0..fields.len]) |*field| {
            allocator.free(field.name);
            allocator.free(field.transform);
        }
    }

    return PartitionSpec{
        .spec_id = spec_id,
        .fields = fields,
        .allocator = allocator,
    };
}

fn parsePartitionField(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !PartitionField {
    const field_id = @as(i32, @intCast(obj.get("field-id").?.integer));
    const source_id = @as(i32, @intCast(obj.get("source-id").?.integer));
    const name = try allocator.dupe(u8, obj.get("name").?.string);
    errdefer allocator.free(name);
    const transform = try allocator.dupe(u8, obj.get("transform").?.string);
    errdefer allocator.free(transform);

    return PartitionField{
        .field_id = field_id,
        .source_id = source_id,
        .name = name,
        .transform = transform,
    };
}

fn parseSnapshot(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !Snapshot {
    const snapshot_id = obj.get("snapshot-id").?.integer;
    const parent_snapshot_id = if (obj.get("parent-snapshot-id")) |val|
        val.integer
    else
        null;
    const sequence_number = obj.get("sequence-number").?.integer;
    const timestamp_ms = obj.get("timestamp-ms").?.integer;
    const manifest_list = try allocator.dupe(u8, obj.get("manifest-list").?.string);
    errdefer allocator.free(manifest_list);
    const schema_id = if (obj.get("schema-id")) |val|
        @as(i32, @intCast(val.integer))
    else
        null;

    // Parse summary (optional)
    var summary: ?SnapshotSummary = null;
    if (obj.get("summary")) |summary_json| {
        const sum_obj = summary_json.object;
        const operation = try allocator.dupe(u8, sum_obj.get("operation").?.string);
        errdefer allocator.free(operation);

        summary = SnapshotSummary{
            .operation = operation,
            .added_data_files = if (sum_obj.get("added-data-files")) |v|
                std.fmt.parseInt(i64, v.string, 10) catch null
            else
                null,
            .deleted_data_files = if (sum_obj.get("deleted-data-files")) |v|
                std.fmt.parseInt(i64, v.string, 10) catch null
            else
                null,
            .added_records = if (sum_obj.get("added-records")) |v|
                std.fmt.parseInt(i64, v.string, 10) catch null
            else
                null,
            .deleted_records = if (sum_obj.get("deleted-records")) |v|
                std.fmt.parseInt(i64, v.string, 10) catch null
            else
                null,
        };
    }

    return Snapshot{
        .snapshot_id = snapshot_id,
        .parent_snapshot_id = parent_snapshot_id,
        .sequence_number = sequence_number,
        .timestamp_ms = timestamp_ms,
        .manifest_list = manifest_list,
        .summary = summary,
        .schema_id = schema_id,
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════
// ICEBERG TABLE READER
// ═══════════════════════════════════════════════════════════════════════════

/// High-level Iceberg table reader
pub const IcebergTable = struct {
    metadata: TableMetadata,
    data_files: std.ArrayList([]const u8), // List of Parquet file paths

    allocator: std.mem.Allocator,

    const Self = @This();

    /// Create a new IcebergTable from metadata
    pub fn init(allocator: std.mem.Allocator, metadata: TableMetadata) Self {
        return .{
            .metadata = metadata,
            .data_files = std.ArrayList([]const u8){},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free data file paths
        for (self.data_files.items) |file_path| {
            self.allocator.free(file_path);
        }
        self.data_files.deinit(self.allocator);
    }

    /// Load data files from current snapshot's manifest
    /// This would normally read the manifest-list Avro file and parse all manifest entries
    /// For now, this is a placeholder that would be called after reading manifest files
    pub fn loadDataFiles(self: *Self, manifest_entries: []const @import("avro.zig").DataFileEntry) !void {
        for (manifest_entries) |*entry| {
            // Only include ADDED or EXISTING files
            if (entry.status == .added or entry.status == .existing) {
                const file_path = try self.allocator.dupe(u8, entry.file_path);
                try self.data_files.append(self.allocator, file_path);
            }
        }
    }

    /// Get list of data files to read
    pub fn getDataFiles(self: *const Self) []const []const u8 {
        return self.data_files.items;
    }

    /// Get current schema
    pub fn getSchema(self: *const Self) ?*const Schema {
        return self.metadata.getCurrentSchema();
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "parse simple iceberg metadata" {
    const allocator = std.testing.allocator;

    const json_data =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        \\  "location": "s3://bucket/warehouse/my_table",
        \\  "last-updated-ms": 1602638573590,
        \\  "last-column-id": 3,
        \\  "current-snapshot-id": 3055729675574597004,
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 0,
        \\      "fields": [
        \\        {
        \\          "id": 1,
        \\          "name": "x",
        \\          "required": true,
        \\          "type": "long"
        \\        },
        \\        {
        \\          "id": 2,
        \\          "name": "y",
        \\          "required": true,
        \\          "type": "long"
        \\        }
        \\      ]
        \\    }
        \\  ],
        \\  "partition-specs": [
        \\    {
        \\      "spec-id": 0,
        \\      "fields": []
        \\    }
        \\  ],
        \\  "snapshots": []
        \\}
    ;

    var metadata = try TableMetadata.parseFromJson(allocator, json_data);
    defer metadata.deinit();

    try std.testing.expectEqual(@as(i32, 2), metadata.format_version);
    try std.testing.expectEqualStrings("9c12d441-03fe-4693-9a96-a0705ddf69c1", metadata.table_uuid);
    try std.testing.expectEqual(@as(i32, 0), metadata.current_schema_id);
    try std.testing.expectEqual(@as(usize, 1), metadata.schemas.len);
    try std.testing.expectEqual(@as(usize, 2), metadata.schemas[0].fields.len);
}

test "snapshot resolution" {
    const allocator = std.testing.allocator;

    const json_data =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "test-uuid",
        \\  "location": "s3://bucket/warehouse/my_table",
        \\  "last-updated-ms": 1602638573590,
        \\  "last-column-id": 1,
        \\  "current-snapshot-id": 100,
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 0,
        \\      "fields": [
        \\        {
        \\          "id": 1,
        \\          "name": "x",
        \\          "required": true,
        \\          "type": "long"
        \\        }
        \\      ]
        \\    }
        \\  ],
        \\  "partition-specs": [
        \\    {
        \\      "spec-id": 0,
        \\      "fields": []
        \\    }
        \\  ],
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 100,
        \\      "sequence-number": 1,
        \\      "timestamp-ms": 1602638573590,
        \\      "manifest-list": "s3://bucket/warehouse/my_table/metadata/snap-100.avro"
        \\    }
        \\  ]
        \\}
    ;

    var metadata = try TableMetadata.parseFromJson(allocator, json_data);
    defer metadata.deinit();

    const current_snapshot = metadata.getCurrentSnapshot();
    try std.testing.expect(current_snapshot != null);
    try std.testing.expectEqual(@as(i64, 100), current_snapshot.?.snapshot_id);
    try std.testing.expectEqualStrings("s3://bucket/warehouse/my_table/metadata/snap-100.avro", current_snapshot.?.manifest_list);
}

test "IcebergTable - load data files from manifest" {
    const allocator = std.testing.allocator;
    const avro = @import("avro.zig");

    const json_data =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "test-uuid",
        \\  "location": "s3://bucket/warehouse/my_table",
        \\  "last-updated-ms": 1602638573590,
        \\  "last-column-id": 1,
        \\  "current-snapshot-id": 100,
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "schemas": [
        \\    {
        \\      "schema-id": 0,
        \\      "fields": [
        \\        {
        \\          "id": 1,
        \\          "name": "id",
        \\          "required": true,
        \\          "type": "long"
        \\        },
        \\        {
        \\          "id": 2,
        \\          "name": "data",
        \\          "required": false,
        \\          "type": "string"
        \\        }
        \\      ]
        \\    }
        \\  ],
        \\  "partition-specs": [
        \\    {
        \\      "spec-id": 0,
        \\      "fields": []
        \\    }
        \\  ],
        \\  "snapshots": [
        \\    {
        \\      "snapshot-id": 100,
        \\      "sequence-number": 1,
        \\      "timestamp-ms": 1602638573590,
        \\      "manifest-list": "s3://bucket/warehouse/my_table/metadata/snap-100.avro"
        \\    }
        \\  ]
        \\}
    ;

    var metadata = try TableMetadata.parseFromJson(allocator, json_data);
    defer metadata.deinit();

    // Create IcebergTable
    var table = IcebergTable.init(allocator, metadata);
    defer table.deinit();

    // Simulate manifest entries (normally read from Avro manifest files)
    const file1_path = "s3://bucket/warehouse/my_table/data/00000-0-data.parquet";
    const file2_path = "s3://bucket/warehouse/my_table/data/00001-1-data.parquet";

    var manifest_entries = [_]avro.DataFileEntry{
        .{
            .status = .added,
            .snapshot_id = 100,
            .file_path = file1_path,
            .file_format = "PARQUET",
            .record_count = 1000,
            .file_size_in_bytes = 4096,
            .allocator = allocator,
        },
        .{
            .status = .added,
            .snapshot_id = 100,
            .file_path = file2_path,
            .file_format = "PARQUET",
            .record_count = 2000,
            .file_size_in_bytes = 8192,
            .allocator = allocator,
        },
    };

    // Load data files
    try table.loadDataFiles(&manifest_entries);

    // Verify
    const data_files = table.getDataFiles();
    try std.testing.expectEqual(@as(usize, 2), data_files.len);
    try std.testing.expectEqualStrings(file1_path, data_files[0]);
    try std.testing.expectEqualStrings(file2_path, data_files[1]);

    // Verify schema access
    const schema = table.getSchema();
    try std.testing.expect(schema != null);
    try std.testing.expectEqual(@as(usize, 2), schema.?.fields.len);
    try std.testing.expectEqualStrings("id", schema.?.fields[0].name);
    try std.testing.expectEqualStrings("data", schema.?.fields[1].name);
}
