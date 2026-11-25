"""Code `api_bronze` and `db_bronze` share (DESIGN-db.md D1).

Only what is the same in both and touches no dlt internals: strict config
building, secret lookup, and the Lambda handler's config loading, error
unwrapping and time budget. Each package keeps its own dlt-facing code, so a
dlt behaviour one of them relies on never binds the other.
"""
