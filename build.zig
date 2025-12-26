const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // === GLACIER MODULE (shared) ===
    const glacier_module = b.addModule("glacier", .{
        .root_source_file = b.path("src/glacier.zig"),
        .target = target,
    });

    // === REPL - Interactive SQL Shell (MAIN EXECUTABLE) ===
    const repl = b.addExecutable(.{
        .name = "glacier",
        .root_module = b.createModule(.{
            .root_source_file = b.path("glacier_repl.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_module },
            },
        }),
    });
    b.installArtifact(repl);

    // Build step for REPL (default)
    const repl_step = b.step("repl", "Build interactive SQL REPL (run with: zig-out/bin/glacier.exe)");
    repl_step.dependOn(&b.addInstallArtifact(repl, .{}).step);

    // === RUN COMMAND ===
    const run_step = b.step("run", "Run the Glacier query engine");
    const run_cmd = b.addRunArtifact(repl);
    run_step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // === TESTS ===
    const lib_tests = b.addTest(.{
        .root_module = glacier_module,
    });

    const run_lib_tests = b.addRunArtifact(lib_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);

    // === BENCHMARK ===
    const bench_exe = b.addExecutable(.{
        .name = "glacier-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    b.installArtifact(bench_exe);

    const bench_step = b.step("bench", "Run benchmarks");
    const bench_cmd = b.addRunArtifact(bench_exe);
    bench_step.dependOn(&bench_cmd.step);

    // === PARQUET-DUMP EXAMPLE ===
    const parquet_dump = b.addExecutable(.{
        .name = "parquet-dump",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/parquet_dump.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_module },
            },
        }),
    });

    b.installArtifact(parquet_dump);

    const parquet_dump_step = b.step("parquet-dump", "Build parquet-dump tool");
    parquet_dump_step.dependOn(b.getInstallStep());
}
