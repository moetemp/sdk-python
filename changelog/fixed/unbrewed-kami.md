Core: Preserve queued queries across workflow eviction so requests for the same run are answered instead of silently dropped.
Core: Dev and test server startup now stops waiting at the configured deadline even if a readiness request stalls. Timeout messages include the last connection failure when available.
Core: Rejection-only update tasks no longer dispatch local activities resumed from replay before validation, allowing those tasks to remain unrecorded in workflow history.
Core: Preparing a legacy query now remains in replay mode, preventing unrecorded local activity attempts that could execute twice or leave the workflow stuck.
