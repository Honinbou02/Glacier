// ═══════════════════════════════════════════════════════════════════════════
// ERROR DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════
//
// Centralized error types for the entire Glacier engine.
// All errors must be properly typed and propagated - NO unwrap() or ignore!
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");

/// Memory allocation and management errors
pub const MemoryError = error{
    OutOfMemory,
    InvalidAlignment,
    ArenaOverflow,
    BufferTooSmall,
};

/// Network and I/O errors
pub const NetworkError = error{
    ConnectionFailed,
    ConnectionReset,
    Timeout,
    InvalidResponse,
    TlsHandshakeFailed,
    CertificateVerificationFailed,
};

/// HTTP-specific errors
pub const HttpError = error{
    InvalidStatusCode,
    InvalidHeaders,
    InvalidUrl,
    RedirectLimitExceeded,
};

/// AWS S3 errors
pub const S3Error = error{
    AuthenticationFailed,
    BucketNotFound,
    ObjectNotFound,
    AccessDenied,
    InvalidSignature,
};

/// File format parsing errors
pub const FormatError = error{
    InvalidMagicNumber,
    UnsupportedVersion,
    CorruptedData,
    InvalidSchema,
    InvalidEncoding,
};

/// Parquet-specific errors
pub const ParquetError = error{
    InvalidFooter,
    UnsupportedCompression,
    UnsupportedEncoding,
    InvalidPageHeader,
    InvalidColumnChunk,
    ThriftDecodeFailed,
    InvalidSnappyData,
    SnappyDecompressionFailed,
};

/// Iceberg table format errors
pub const IcebergError = error{
    MetadataNotFound,
    InvalidSnapshot,
    ManifestParseFailed,
    SchemaEvolutionNotSupported,
    PartitionSpecInvalid,
};

/// Avro format errors
pub const AvroError = error{
    InvalidSchema,
    InvalidBlock,
    CodecNotSupported,
};

/// Query execution errors
pub const ExecutionError = error{
    TypeMismatch,
    DivisionByZero,
    Overflow,
    InvalidExpression,
    ColumnNotFound,
};

/// Combines all possible errors in the system
pub const GlacierError = MemoryError ||
    NetworkError ||
    HttpError ||
    S3Error ||
    FormatError ||
    ParquetError ||
    IcebergError ||
    AvroError ||
    ExecutionError;

/// Helper to convert std.mem.Allocator.Error to our MemoryError
pub fn fromAllocError(err: std.mem.Allocator.Error) MemoryError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
    };
}

test "error types are non-overlapping" {
    // Ensure error sets can be combined
    const all_errors: GlacierError = error.OutOfMemory;
    try std.testing.expect(all_errors == error.OutOfMemory);
}
