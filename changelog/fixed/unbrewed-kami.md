Core: Preserve queued queries across workflow eviction so requests for the same run are answered instead of silently dropped.
Core: Dev and test server startup now stops waiting at the configured deadline even if a readiness request stalls. Timeout messages include the last connection failure when available.
Core: Rejection-only update tasks no longer dispatch local activities resumed from replay before validation, allowing those tasks to remain unrecorded in workflow history.
Core: Preparing a legacy query now remains in replay mode, preventing unrecorded local activity attempts that could execute twice or leave the workflow stuck.
Core: Resource-based worker tuning now reads CPU usage and quotas from standard cgroup v1 mounts, preventing host CPU measurements from understating container load when cgroup v2 files are unavailable.
Core: Prevent Nexus worker shutdown from hanging if a task completion future is dropped or its response is rejected as malformed. Dropping an in-flight completion logs an error and panics when debug assertions are enabled; callers must still await completion to preserve RPC delivery.
