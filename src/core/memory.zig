// ═══════════════════════════════════════════════════════════════════════════
// MEMORY MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════
//
// Arena-based memory allocation strategy.
//
// PRINCIPLES:
//   1. NO malloc/free in hot path (query execution)
//   2. Hierarchical arenas: Query → Batch → Column
//   3. All memory freed in O(1) when arena is destroyed
//   4. Predictable memory usage - no fragmentation
//
// LIFE-CYCLE:
//   QueryArena.init()     → Allocate large chunk (e.g., 64MB)
//     ├─ MetadataArena    → Schema, stats, etc (small)
//     ├─ BatchArena       → Intermediate results (medium)
//     └─ BufferArena      → I/O buffers (large)
//   QueryArena.deinit()   → Free everything in 1 syscall
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const errors = @import("errors.zig");

/// Configuration for memory limits
pub const MemoryConfig = struct {
    /// Maximum memory per query (default: 256MB)
    max_query_memory: usize = 256 * 1024 * 1024,

    /// Size of I/O buffers for network/disk reads (default: 1MB)
    io_buffer_size: usize = 1024 * 1024,

    /// Size of decompression buffers (default: 4MB)
    decompress_buffer_size: usize = 4 * 1024 * 1024,

    /// Number of rows per batch in vectorized processing (default: 4096)
    batch_size: usize = 4096,
};

/// Default memory configuration
pub const default_config = MemoryConfig{};

/// Hierarchical arena allocator for query execution
pub const QueryArena = struct {
    arena: std.heap.ArenaAllocator,
    config: MemoryConfig,

    const Self = @This();

    /// Initialize a new query arena with the given backing allocator
    pub fn init(backing_allocator: std.mem.Allocator, config: MemoryConfig) Self {
        return .{
            .arena = std.heap.ArenaAllocator.init(backing_allocator),
            .config = config,
        };
    }

    /// Get the allocator interface
    pub fn allocator(self: *Self) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// Allocate memory with tracking and limit enforcement
    pub fn alloc(self: *Self, comptime T: type, n: usize) errors.MemoryError![]T {
        const byte_count = @sizeOf(T) * n;

        // Check memory limit BEFORE allocating (pessimistic check)
        // Use queryCapacity() to get REAL memory usage (not logical)
        const current_usage = self.getBytesAllocated();
        if (current_usage + byte_count > self.config.max_query_memory) {
            return error.ArenaOverflow;
        }

        const result = self.arena.allocator().alloc(T, n) catch |err| {
            return errors.fromAllocError(err);
        };

        return result;
    }

    /// Allocate a single item
    pub fn create(self: *Self, comptime T: type) errors.MemoryError!*T {
        const byte_count = @sizeOf(T);

        // Check memory limit using real capacity
        const current_usage = self.getBytesAllocated();
        if (current_usage + byte_count > self.config.max_query_memory) {
            return error.ArenaOverflow;
        }

        const result = self.arena.allocator().create(T) catch |err| {
            return errors.fromAllocError(err);
        };

        return result;
    }

    /// Get REAL memory usage (including overhead and fragmentation)
    /// This uses the arena's internal state, not just logical accounting
    pub fn getBytesAllocated(self: *const Self) usize {
        // ArenaAllocator tracks total bytes requested from backing allocator
        // This is the REAL memory usage, not just sum of alloc requests
        return self.arena.queryCapacity();
    }

    /// Free all memory allocated in this arena (O(1) operation)
    pub fn deinit(self: *Self) void {
        self.arena.deinit();
    }
};

/// Re-export std.heap.FixedBufferAllocator for convenience
/// Used for I/O buffers and decompression - prevents dynamic allocation
///
/// The standard library implementation is tested, optimized, and handles
/// alignment and resize better than any custom implementation.
///
/// Usage:
///   var buffer: [1024]u8 = undefined;
///   var fba = std.heap.FixedBufferAllocator.init(&buffer);
///   const allocator = fba.allocator();
pub const FixedBufferAllocator = std.heap.FixedBufferAllocator;

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "QueryArena basic allocation" {
    var arena = QueryArena.init(std.testing.allocator, default_config);
    defer arena.deinit();

    const items = try arena.alloc(u64, 100);
    try std.testing.expectEqual(@as(usize, 100), items.len);

    // getBytesAllocated() now returns REAL capacity, not logical size
    // Real capacity includes overhead (e.g., 4KB chunk even for 800 bytes)
    const actual_usage = arena.getBytesAllocated();
    try std.testing.expect(actual_usage >= 800); // At least the logical size
}

test "QueryArena memory limit enforcement" {
    var config = default_config;
    config.max_query_memory = 1024; // Only 1KB allowed

    var arena = QueryArena.init(std.testing.allocator, config);
    defer arena.deinit();

    // This should succeed (800 bytes)
    _ = try arena.alloc(u64, 100);

    // This should fail (would exceed 1KB)
    const result = arena.alloc(u64, 100);
    try std.testing.expectError(error.ArenaOverflow, result);
}

test "FixedBufferAllocator basic usage" {
    var buffer: [1024]u8 = undefined;
    var fba = FixedBufferAllocator.init(&buffer);

    const alloc = fba.allocator();

    const slice1 = try alloc.alloc(u8, 100);
    try std.testing.expectEqual(@as(usize, 100), slice1.len);

    const slice2 = try alloc.alloc(u64, 10);
    try std.testing.expectEqual(@as(usize, 10), slice2.len);

    // Verify alignment
    try std.testing.expect(@intFromPtr(slice2.ptr) % @alignOf(u64) == 0);
}

test "FixedBufferAllocator overflow" {
    var buffer: [100]u8 = undefined;
    var fba = FixedBufferAllocator.init(&buffer);

    const alloc = fba.allocator();

    // Should fail - not enough space
    const result = alloc.alloc(u8, 200);
    try std.testing.expectError(error.OutOfMemory, result);
}

test "FixedBufferAllocator reset" {
    var buffer: [1024]u8 = undefined;
    var fba = FixedBufferAllocator.init(&buffer);

    const alloc = fba.allocator();

    _ = try alloc.alloc(u8, 500);
    // std.heap.FixedBufferAllocator tracks end_index internally
    // We can't access it directly, but we can verify behavior

    fba.reset();

    // After reset, should be able to allocate the same amount again
    _ = try alloc.alloc(u8, 500);
}
