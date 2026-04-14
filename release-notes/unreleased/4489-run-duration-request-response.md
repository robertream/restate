# Release Notes for Issue #4489: Run duration accuracy in request-response mode

## Bug Fix

### What Changed
`ctx.run` durations are now reported accurately in request-response mode (e.g., Lambda deployments).
Previously, all run durations appeared as 0ms because journal entries arrived in a single batch
with identical timestamps. The server now accepts an optional SDK-measured `attempt_duration_ms`
on run completion proposals and uses it for trace span timing.

### Why This Matters
Users deploying handlers via Lambda or other request-response transports could not see meaningful
`ctx.run` durations in the journal timeline, making it difficult to identify slow side effects.

### Impact on Users
- No action required. SDKs that report `attempt_duration_ms` will automatically produce accurate
  run durations. Older SDKs continue to work with the existing timestamp-based behavior.

### Related Issues
- Issue #4489
