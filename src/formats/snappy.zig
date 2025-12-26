//! SNAPPY decompression (Parquet variant)
//!
//! Parquet uses raw SNAPPY blocks (not framed format)
//! Reference: https://github.com/google/snappy/blob/main/format_description.txt

const std = @import("std");

/// Decompress SNAPPY data
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) ![]u8 {
    var output = try allocator.alloc(u8, uncompressed_size);
    errdefer allocator.free(output);

    var in_pos: usize = 0;
    var out_pos: usize = 0;

    // Skip uncompressed length varint at start (we already know the size from PageHeader)
    while (in_pos < compressed.len) : (in_pos += 1) {
        if ((compressed[in_pos] & 0x80) == 0) {
            in_pos += 1;
            break;
        }
    }

    // Decompress blocks
    while (in_pos < compressed.len) {
        const tag = compressed[in_pos];
        in_pos += 1;

        const tag_type = tag & 0x03;

        if (tag_type == 0x00) {
            // Literal: copy bytes directly
            const length = @as(usize, tag >> 2) + 1;
            const literal_len = if (length < 60) blk: {
                break :blk length;
            } else blk: {
                // Extended length
                const extra_bytes = length - 59;
                var len: usize = 0;
                for (0..extra_bytes) |i| {
                    len |= @as(usize, compressed[in_pos + i]) << @intCast(i * 8);
                }
                in_pos += extra_bytes;
                break :blk len + 1;
            };

            // Copy literal bytes
            if (in_pos + literal_len > compressed.len or out_pos + literal_len > output.len) {
                return error.SnappyDecompressionFailed;
            }
            @memcpy(output[out_pos..][0..literal_len], compressed[in_pos..][0..literal_len]);
            in_pos += literal_len;
            out_pos += literal_len;
        } else {
            // Copy: reference to earlier data
            var offset: usize = 0;
            var length: usize = 0;

            if (tag_type == 0x01) {
                // 1-byte offset, 4-bit length
                length = ((tag >> 2) & 0x07) + 4;
                offset = (@as(usize, tag & 0xE0) << 3) | @as(usize, compressed[in_pos]);
                in_pos += 1;
            } else if (tag_type == 0x02) {
                // 2-byte offset, 6-bit length
                length = (@as(usize, tag >> 2) + 1);
                if (in_pos + 1 >= compressed.len) return error.SnappyDecompressionFailed;
                offset = @as(usize, compressed[in_pos]) | (@as(usize, compressed[in_pos + 1]) << 8);
                in_pos += 2;
            } else {
                // 4-byte offset
                length = (@as(usize, tag >> 2) + 1);
                if (in_pos + 3 >= compressed.len) return error.SnappyDecompressionFailed;
                offset = @as(usize, compressed[in_pos]) |
                    (@as(usize, compressed[in_pos + 1]) << 8) |
                    (@as(usize, compressed[in_pos + 2]) << 16) |
                    (@as(usize, compressed[in_pos + 3]) << 24);
                in_pos += 4;
            }

            // Copy from earlier in output
            if (offset == 0 or offset > out_pos or out_pos + length > output.len) {
                return error.SnappyDecompressionFailed;
            }

            const copy_from = out_pos - offset;
            for (0..length) |i| {
                output[out_pos + i] = output[copy_from + i];
            }
            out_pos += length;
        }
    }

    if (out_pos != uncompressed_size) {
        return error.SnappyDecompressionFailed;
    }

    return output;
}
