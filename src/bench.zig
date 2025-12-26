// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Benchmark Suite
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const glacier = @import("glacier.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("╔══════════════════════════════════════════════════════════════════╗\n", .{});
    std.debug.print("║                   GLACIER - Benchmarks                         ║\n", .{});
    std.debug.print("╚══════════════════════════════════════════════════════════════════╝\n\n", .{});

    try benchmarkMemoryArena(allocator);
    benchmarkRingBuffer();
}

fn benchmarkMemoryArena(allocator: std.mem.Allocator) !void {
    std.debug.print("Benchmark: Memory Arena Allocation\n", .{});

    const iterations = 10000;
    var timer = try std.time.Timer.start();

    const config = glacier.memory.default_config;

    for (0..iterations) |_| {
        var arena = glacier.memory.QueryArena.init(allocator, config);
        defer arena.deinit();

        _ = try arena.alloc(u64, 1000);
    }

    const elapsed = timer.read();
    const ns_per_iter = elapsed / iterations;

    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time per iteration: {d} ns\n", .{ns_per_iter});
    std.debug.print("  Throughput: {d:.2} ops/sec\n\n", .{@as(f64, @floatFromInt(std.time.ns_per_s)) / @as(f64, @floatFromInt(ns_per_iter))});
}

fn benchmarkRingBuffer() void {
    std.debug.print("Benchmark: Ring Buffer Throughput\n", .{});

    var backing: [64 * 1024]u8 = undefined;
    var ring = glacier.buffers.RingBuffer.init(&backing);

    const test_data = "x" ** 1024; // 1KB
    const iterations = 100000;

    var timer = std.time.Timer.start() catch return;

    for (0..iterations) |_| {
        _ = ring.write(test_data);
        var buf: [1024]u8 = undefined;
        _ = ring.read(&buf);
    }

    const elapsed = timer.read();
    const bytes_processed = test_data.len * iterations * 2; // write + read
    const throughput_mb = @as(f64, @floatFromInt(bytes_processed)) / @as(f64, @floatFromInt(elapsed)) * @as(f64, std.time.ns_per_s) / (1024 * 1024);

    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Bytes processed: {d} MB\n", .{bytes_processed / (1024 * 1024)});
    std.debug.print("  Throughput: {d:.2} MB/s\n\n", .{throughput_mb});
}
