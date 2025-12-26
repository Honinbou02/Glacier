// ═══════════════════════════════════════════════════════════════════════════
// AWS SIGNATURE V4 AUTHENTICATION
// ═══════════════════════════════════════════════════════════════════════════
//
// Manual implementation of AWS Signature Version 4 for S3 requests.
//
// ALGORITHM:
//   1. Create Canonical Request (HTTP method + URI + headers + payload hash)
//   2. Create String to Sign (timestamp + region + service + hashed canonical request)
//   3. Calculate Signature (HMAC-SHA256 chain)
//   4. Add Authorization header
//
// REFERENCES:
//   - https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
//   - https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const errors = @import("../core/errors.zig");

/// AWS credentials
pub const Credentials = struct {
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8 = null, // For temporary credentials
};

/// AWS region information
pub const Region = struct {
    name: []const u8, // e.g., "us-east-1"

    pub const US_EAST_1 = Region{ .name = "us-east-1" };
    pub const US_WEST_2 = Region{ .name = "us-west-2" };
    pub const EU_WEST_1 = Region{ .name = "eu-west-1" };
};

/// S3 request information needed for signing
pub const S3Request = struct {
    method: []const u8, // GET, PUT, POST, DELETE
    bucket: []const u8,
    key: []const u8, // Object key (path)
    query_params: []const QueryParam = &.{}, // Query string parameters
    headers: []const Header = &.{}, // Additional headers
    payload: []const u8 = &.{}, // Request body (empty for GET)

    pub const QueryParam = struct {
        name: []const u8,
        value: []const u8,
    };

    pub const Header = struct {
        name: []const u8,
        value: []const u8,
    };
};

/// Signature V4 signer
pub const Signer = struct {
    credentials: Credentials,
    region: Region,
    service: []const u8, // Usually "s3"
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, credentials: Credentials, region: Region) Self {
        return .{
            .credentials = credentials,
            .region = region,
            .service = "s3",
            .allocator = allocator,
        };
    }

    /// Sign a request and return the Authorization header value
    pub fn sign(self: *const Self, request: S3Request, timestamp: i64) ![]const u8 {
        // Format timestamp as ISO8601
        const datetime = try self.formatDateTime(timestamp);
        const date = datetime[0..8]; // YYYYMMDD

        // Step 1: Create canonical request
        const canonical_request = try self.createCanonicalRequest(request, datetime);
        defer self.allocator.free(canonical_request);

        // Step 2: Create string to sign
        const string_to_sign = try self.createStringToSign(canonical_request, datetime, date);
        defer self.allocator.free(string_to_sign);

        // Step 3: Calculate signing key
        const signing_key = try self.calculateSigningKey(date);

        // Step 4: Calculate signature
        const signature = try self.calculateSignature(&signing_key, string_to_sign);
        defer self.allocator.free(signature);

        // Step 5: Create authorization header
        const credential_scope = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}/aws4_request",
            .{ date, self.region.name, self.service },
        );
        defer self.allocator.free(credential_scope);

        const auth_header = try std.fmt.allocPrint(
            self.allocator,
            "AWS4-HMAC-SHA256 Credential={s}/{s}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={s}",
            .{ self.credentials.access_key_id, credential_scope, signature },
        );

        return auth_header;
    }

    /// Format Unix timestamp as ISO8601 (YYYYMMDDTHHMMSSZ)
    fn formatDateTime(self: *const Self, timestamp: i64) ![]const u8 {
        const epoch_seconds: u64 = @intCast(timestamp);
        const epoch_day: u47 = @intCast(epoch_seconds / std.time.s_per_day);
        const day_seconds = epoch_seconds % std.time.s_per_day;

        const year_day = std.time.epoch.EpochDay{ .day = epoch_day };
        const year = year_day.calculateYearDay();
        const month_day = year.calculateMonthDay();

        const hours = day_seconds / std.time.s_per_hour;
        const minutes = (day_seconds % std.time.s_per_hour) / std.time.s_per_min;
        const seconds = day_seconds % std.time.s_per_min;

        return try std.fmt.allocPrint(
            self.allocator,
            "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z",
            .{
                year.year,
                month_day.month.numeric(),
                month_day.day_index + 1,
                hours,
                minutes,
                seconds,
            },
        );
    }

    /// Create canonical request (Task 1 of signing process)
    fn createCanonicalRequest(self: *const Self, request: S3Request, datetime: []const u8) ![]const u8 {
        // Hash the payload
        var payload_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(request.payload, &payload_hash, .{});
        const payload_hash_hex = try self.hexEncode(&payload_hash);
        defer self.allocator.free(payload_hash_hex);

        // Build canonical URI
        const canonical_uri = try self.buildCanonicalUri(request.key);
        defer self.allocator.free(canonical_uri);

        // Build canonical query string
        const canonical_query = try self.buildCanonicalQueryString(request.query_params);
        defer self.allocator.free(canonical_query);

        // Build canonical headers
        const host = try std.fmt.allocPrint(self.allocator, "{s}.s3.{s}.amazonaws.com", .{ request.bucket, self.region.name });
        defer self.allocator.free(host);

        const canonical_headers = try std.fmt.allocPrint(
            self.allocator,
            "host:{s}\nx-amz-content-sha256:{s}\nx-amz-date:{s}\n",
            .{ host, payload_hash_hex, datetime },
        );
        defer self.allocator.free(canonical_headers);

        const signed_headers = "host;x-amz-content-sha256;x-amz-date";

        // Combine into canonical request
        const canonical_request = try std.fmt.allocPrint(
            self.allocator,
            "{s}\n{s}\n{s}\n{s}\n{s}\n{s}",
            .{
                request.method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash_hex,
            },
        );

        return canonical_request;
    }

    fn buildCanonicalUri(self: *const Self, key: []const u8) ![]const u8 {
        if (key.len == 0) return try self.allocator.dupe(u8, "/");

        // URI encode the key
        var result: std.ArrayListUnmanaged(u8) = .{};
        defer result.deinit(self.allocator);

        try result.append(self.allocator, '/');

        for (key) |c| {
            if (std.ascii.isAlphanumeric(c) or c == '-' or c == '_' or c == '.' or c == '~' or c == '/') {
                try result.append(self.allocator, c);
            } else {
                var buf: [3]u8 = undefined;
                const encoded = try std.fmt.bufPrint(&buf, "%{X:0>2}", .{c});
                try result.appendSlice(self.allocator, encoded);
            }
        }

        return result.toOwnedSlice(self.allocator);
    }

    fn buildCanonicalQueryString(self: *const Self, params: []const S3Request.QueryParam) ![]const u8 {
        if (params.len == 0) return try self.allocator.dupe(u8, "");

        // TODO: Sort params and URI encode
        // For now, return empty string
        return try self.allocator.dupe(u8, "");
    }

    /// Create string to sign (Task 2)
    fn createStringToSign(self: *const Self, canonical_request: []const u8, datetime: []const u8, date: []const u8) ![]const u8 {
        // Hash the canonical request
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(canonical_request, &hash, .{});
        const hash_hex = try self.hexEncode(&hash);
        defer self.allocator.free(hash_hex);

        const credential_scope = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}/aws4_request",
            .{ date, self.region.name, self.service },
        );
        defer self.allocator.free(credential_scope);

        return try std.fmt.allocPrint(
            self.allocator,
            "AWS4-HMAC-SHA256\n{s}\n{s}\n{s}",
            .{ datetime, credential_scope, hash_hex },
        );
    }

    /// Calculate signing key (Task 3)
    fn calculateSigningKey(self: *const Self, date: []const u8) ![32]u8 {
        const secret = try std.fmt.allocPrint(self.allocator, "AWS4{s}", .{self.credentials.secret_access_key});
        defer self.allocator.free(secret);

        var k_date: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&k_date, date, secret);

        var k_region: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&k_region, self.region.name, &k_date);

        var k_service: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&k_service, self.service, &k_region);

        var k_signing: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&k_signing, "aws4_request", &k_service);

        return k_signing;
    }

    /// Calculate signature (Task 4)
    fn calculateSignature(self: *const Self, signing_key: *const [32]u8, string_to_sign: []const u8) ![]const u8 {
        var sig: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&sig, string_to_sign, signing_key);

        return try std.fmt.allocPrint(self.allocator, "{x}", .{sig});
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "AWS datetime formatting" {
    const signer = Signer.init(
        std.testing.allocator,
        .{ .access_key_id = "test", .secret_access_key = "test" },
        Region.US_EAST_1,
    );

    const timestamp: i64 = 1234567890; // 2009-02-13 23:31:30 UTC
    const datetime = try signer.formatDateTime(timestamp);
    defer std.testing.allocator.free(datetime);

    try std.testing.expectEqualStrings("20090213T233130Z", datetime);
}

test "Canonical URI encoding" {
    const signer = Signer.init(
        std.testing.allocator,
        .{ .access_key_id = "test", .secret_access_key = "test" },
        Region.US_EAST_1,
    );

    const uri = try signer.buildCanonicalUri("my-object.txt");
    defer std.testing.allocator.free(uri);

    try std.testing.expectEqualStrings("/my-object.txt", uri);
}

test "Hex encoding" {
    const bytes = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    const hex = try std.fmt.allocPrint(std.testing.allocator, "{x}", .{bytes});
    defer std.testing.allocator.free(hex);

    try std.testing.expectEqualStrings("deadbeef", hex);
}
