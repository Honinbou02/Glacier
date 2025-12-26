const std = @import("std");
const parquet = @import("parquet.zig");

/// Decompress data based on codec
pub fn decompress(
    allocator: std.mem.Allocator,
    codec: parquet.CompressionCodec,
    compressed: []const u8,
    decompressed_size: usize,
) ![]u8 {
    return switch (codec) {
        .UNCOMPRESSED => try allocator.dupe(u8, compressed),
        .SNAPPY => try decompressSnappy(allocator, compressed, decompressed_size),
        else => {
            std.debug.print("Unsupported compression codec: {any}\n", .{codec});
            return error.UnsupportedCompression;
        },
    };
}

// PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
// SNAPPY DECOMPRESSION (Raw Snappy - used by Parquet)
// PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
//
// Parquet uses RAW Snappy compression (no framing, no headers).
// The uncompressed size is provided by the Parquet page header.
//
// Snappy format:
//   - Stream of literals and copy operations
//   - Tag byte determines operation type
//   - No length prefix, no checksums
//
// PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP

fn decompressSnappy(
    allocator: std.mem.Allocator,
    compressed: []const u8,
    expected_size: usize,
) ![]u8 {
    const output = try allocator.alloc(u8, expected_size);
    errdefer allocator.free(output);

    var in_pos: usize = 0;
    var out_pos: usize = 0;

    // Skip uncompressed length varint at start (Parquet format)
    while (in_pos < compressed.len) {
        const byte = compressed[in_pos];
        in_pos += 1;
        if ((byte & 0x80) == 0) {
            break;
        }
    }

    // Decode raw Snappy stream
    while (in_pos < compressed.len and out_pos < expected_size) {

        const tag = compressed[in_pos];
        in_pos += 1;
        const tag_type = tag & 0x03;

        switch (tag_type) {
            0 => { // Literal
                const tag_len: usize = @as(usize, tag >> 2);
                var len: usize = tag_len + 1;

                // Handle extended literal lengths (tag_len >= 60)
                if (tag_len >= 60) {
                    const extra_bytes = tag_len - 59;  // 60->1, 61->2, 62->3, 63->4
                    if (in_pos + extra_bytes - 1 >= compressed.len) return error.InvalidSnappyData;

                    len = 0;
                    for (0..extra_bytes) |i| {
                        len |= @as(usize, compressed[in_pos + i]) << @intCast(i * 8);
                    }
                    len += 1;  // Spec: actual length is stored value + 1
                    in_pos += extra_bytes;
                }

                // Copy literal bytes
                if (in_pos + len > compressed.len or out_pos + len > output.len) {
                    return error.InvalidSnappyData;
                }

                @memcpy(output[out_pos..][0..len], compressed[in_pos..][0..len]);
                in_pos += len;
                out_pos += len;
            },
            1 => { // Copy 1-byte offset
                if (in_pos >= compressed.len) return error.InvalidSnappyData;

                const len: usize = 4 + @as(usize, (tag >> 2) & 0x07);
                const offset: usize = (@as(usize, tag & 0xE0) << 3) | @as(usize, compressed[in_pos]);
                in_pos += 1;

                if (offset == 0 or offset > out_pos or out_pos + len > output.len) {
                    return error.InvalidSnappyData;
                }

                // Copy from earlier in output
                var i: usize = 0;
                while (i < len) : (i += 1) {
                    output[out_pos + i] = output[out_pos - offset + i];
                }
                out_pos += len;
            },
            2 => { // Copy 2-byte offset
                const len: usize = 1 + @as(usize, tag >> 2);
                if (in_pos + 1 >= compressed.len) return error.InvalidSnappyData;

                const offset = @as(usize, std.mem.readInt(u16, compressed[in_pos..][0..2], .little));
                in_pos += 2;

                if (offset == 0 or offset > out_pos or out_pos + len > output.len) {
                    return error.InvalidSnappyData;
                }

                // Copy from earlier in output
                var i: usize = 0;
                while (i < len) : (i += 1) {
                    output[out_pos + i] = output[out_pos - offset + i];
                }
                out_pos += len;
            },
            3 => { // Copy 4-byte offset
                const len: usize = 1 + @as(usize, tag >> 2);
                if (in_pos + 4 > compressed.len) return error.InvalidSnappyData;

                const offset = @as(usize, std.mem.readInt(u32, compressed[in_pos..][0..4], .little));
                in_pos += 4;

                if (offset == 0 or offset > out_pos or out_pos + len > output.len) {
                    return error.InvalidSnappyData;
                }

                // Copy from earlier in output
                var i: usize = 0;
                while (i < len) : (i += 1) {
                    output[out_pos + i] = output[out_pos - offset + i];
                }
                out_pos += len;
            },
            else => {
                // Invalid tag type (should never happen since tag & 0x03 only gives 0-3)
                return error.InvalidSnappyData;
            },
        }
    }

    // Validate final decompressed size
    if (out_pos != expected_size) {
        return error.InvalidSnappyData;
    }

    return output;
}

// PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
// TESTS
// PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP

test "uncompressed passthrough" {
    const allocator = std.testing.allocator;
    const data = "Hello, World!";

    const result = try decompress(allocator, .UNCOMPRESSED, data, data.len);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(data, result);
}
