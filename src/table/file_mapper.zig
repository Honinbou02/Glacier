const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

// Page size constant for alignment
const page_size = 4096;

// Windows API declarations for mmap
const windows = std.os.windows;
const HANDLE = windows.HANDLE;
const DWORD = windows.DWORD;
const BOOL = windows.BOOL;

extern "kernel32" fn CreateFileMappingW(
    hFile: HANDLE,
    lpFileMappingAttributes: ?*anyopaque,
    flProtect: DWORD,
    dwMaximumSizeHigh: DWORD,
    dwMaximumSizeLow: DWORD,
    lpName: ?[*:0]const u16,
) callconv(std.builtin.CallingConvention.winapi) ?HANDLE;

extern "kernel32" fn MapViewOfFile(
    hFileMappingObject: HANDLE,
    dwDesiredAccess: DWORD,
    dwFileOffsetHigh: DWORD,
    dwFileOffsetLow: DWORD,
    dwNumberOfBytesToMap: usize,
) callconv(std.builtin.CallingConvention.winapi) ?*anyopaque;

extern "kernel32" fn UnmapViewOfFile(
    lpBaseAddress: *const anyopaque,
) callconv(std.builtin.CallingConvention.winapi) BOOL;

const PAGE_READONLY: DWORD = 0x02;
const FILE_MAP_READ: DWORD = 0x0004;

/// Memory-mapped file handle
pub const MappedFile = struct {
    data: []align(page_size) const u8,
    file: std.fs.File,

    /// Map a file into memory using mmap
    pub fn init(path: []const u8) !MappedFile {
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();

        const file_size = try file.getEndPos();
        if (file_size == 0) {
            return error.EmptyFile;
        }

        // Memory map the file for zero-copy access
        // On Windows, we need to use a different approach since posix.mmap
        // has platform-specific behavior
        const mapped_ptr = switch (builtin.os.tag) {
            .windows => try mmapWindows(file, file_size),
            else => try mmapPosix(file, file_size),
        };

        return MappedFile{
            .data = mapped_ptr,
            .file = file,
        };
    }

    pub fn deinit(self: *MappedFile) void {
        switch (builtin.os.tag) {
            .windows => munmapWindows(self.data),
            else => posix.munmap(self.data),
        }
        self.file.close();
    }
};

// Windows-specific mmap using MapViewOfFile
fn mmapWindows(file: std.fs.File, size: u64) ![]align(page_size) const u8 {
    // Create file mapping
    const size_high: DWORD = @truncate(size >> 32);
    const size_low: DWORD = @truncate(size & 0xFFFFFFFF);

    const mapping = CreateFileMappingW(
        file.handle,
        null,
        PAGE_READONLY,
        size_high,
        size_low,
        null,
    ) orelse return error.CreateFileMappingFailed;
    defer windows.CloseHandle(mapping);

    // Map view of file
    const ptr = MapViewOfFile(
        mapping,
        FILE_MAP_READ,
        0,
        0,
        size,
    ) orelse return error.MapViewOfFileFailed;

    const aligned_ptr: [*]align(page_size) const u8 = @ptrCast(@alignCast(ptr));
    return aligned_ptr[0..size];
}

fn munmapWindows(data: []align(page_size) const u8) void {
    _ = UnmapViewOfFile(@ptrCast(data.ptr));
}

// POSIX mmap (Linux, macOS, etc)
fn mmapPosix(file: std.fs.File, size: u64) ![]align(page_size) const u8 {
    // For POSIX systems, we can use posix.mmap directly
    // The MAP flags are handled internally by the posix module
    const ptr = try posix.mmap(
        null,
        size,
        posix.PROT.READ,
        .{ .TYPE = .PRIVATE },
        file.handle,
        0,
    );
    return ptr;
}
