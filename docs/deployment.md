# Deploying on Lambda

Build one image per package (and per architecture, for api) and use it for every source
of that package. A source is a config document in S3 plus a function.

## Function settings

| Setting | api_bronze | db_bronze | Why |
|---|---|---|---|
| Package | `docker/Dockerfile.api` (arm64 or x86_64) | `docker/Dockerfile.db` (x86_64 only) | dlt with pyarrow is far over the 250 MB zip limit |
| Handler (image default) | `api_bronze.handler.handler` | `db_bronze.handler.handler` | |
| Timeout | 900 s | 900 s; at least the margin plus the slowest page | Split loading uses the remaining time; runs that cannot stop early must finish in it |
| Memory | Start at 1–2 GB; measure. A page is held several times over (bytes, text, parsed, Arrow): size to the largest page | ~1 GB covers the fixed cost; measure the largest table ([Memory sizing](#memory-sizing)) | |
| Ephemeral storage | 2–10 GB | 2–10 GB | dlt buffers files in `/tmp` before upload |
| Reserved concurrency | **1** per incremental function | **1** per incremental function | Two concurrent runs of one pipeline both read state, and the last writer wins |
| Network | Outbound to the API | VPC with a route to the database, and to S3 and Secrets Manager (endpoints or NAT) | |
| IAM | Read `CONFIG_URI`; list/read/write the bronze and quarantine prefixes; `secretsmanager:GetSecretValue` on the referenced secrets | Same, bronze prefix only (incl. `_manifests`) | Confirm the minimal S3 action set on the first real-S3 run |

## Environment

| Variable | Meaning |
|---|---|
| `CONFIG_URI` | Where `source_id` configs live (fsspec URL, e.g. `s3://config-bucket/sources`); config is read from `${CONFIG_URI}/${source_id}.json`. Required with `source_id`. |
| `SAFETY_MARGIN_S` | Seconds kept free before the Lambda timeout. Default 120; the budget is never under 60 s. |
| `LOG_LEVEL` | Default `INFO`. |
| `ORACLE_CLIENT_LIB_DIR` | db image only, set by the image: Instant Client directory. When set, oracledb enters thick mode at import and a broken client fails the cold start. |
| `DATA_WRITER__COMPRESSION` | Set from `parquet_compression` on every run; don't set it by hand. |

Both images set `DLT_DATA_DIR=/tmp/dlt`, `DLT_PROJECT_DIR=/tmp` (only `/tmp` is
writable) and turn dlt telemetry off.

## Images

`docker/Dockerfile.api` builds the api image (arm64 or x86_64):

```sh
mise run docker:lock             # docker/requirements-api.lock from uv.lock
mise run docker:build            # lock + image (-t tag, -p linux/arm64|linux/amd64)
mise run docker:smoke            # invoked through the Lambda emulator
```

One **x86_64** image holds every driver (`docker/Dockerfile.db`, about 1.2-1.5 GB):
`msodbcsql18` and unixODBC, Oracle Instant Client Basic (thick mode, always), ibm_db's
clidriver (x86_64 wheels only, hence no arm64 build), PyMySQL, psycopg. Build arguments
(`MSSQL_REPO_URL`, `ORACLE_IC_URL`, `ORACLE_IC_SHA256`) point at internal mirrors.
`CMD` is `db_bronze.handler.handler`. Least privilege is enforced per function through
IAM and a read-only database user, not by the image.

```sh
mise run docker:lock --db        # docker/requirements-db.lock from uv.lock
mise run docker:build --db       # lock + image (linux/amd64 only)
mise run docker:smoke --db       # drivers, Instant Client, SQLite on both backends
```

What goes into the db image, and why, is in [db/design.md](db/design.md#packaging).

## Memory sizing

api: a page is held several times over (bytes, decoded text, parsed objects, Arrow
table), so size to the largest page. Not yet measured on Lambda with large pages.

db: ConnectorX's stream channel is **unbounded**: if Parquet writing is slower than the
database read, batches pile up in memory, and `batch_size` and the Lambda memory are the
only controls (no backpressure is built). Oracle's ConnectorX source reads the whole
result before the first batch, so peak memory is the whole result: an Oracle table
larger than memory must use keyset (or `pyarrow`). Size each function from a measured
`validate`-plus-run on its largest table (peak RSS). Reference points: the fixed cost of
a run is about 590 MiB for the image container under emulation (an overestimate) and
about 254 MiB RSS in-process, so 1 GB leaves room; the per-table figure on the largest
real table has **not** been measured yet.

## Orchestration

- **Steady state:** schedule `{"source_id": ...}`. With split loading, a Step Function
  loops while `is_empty` is false.
- **Separate pipeline name for paged and parallel runs.** Backfill windows run through
  the same function execute one at a time. To run windows in parallel, or to run a db
  paged snapshot, deploy a second function with a copy of the config whose
  `pipeline_name` differs (e.g. `vendor_orders_backfill`, `core_banking_snapshot`) and
  no reserved concurrency. Backfills never write the cursor, but they still write dlt
  state rows, so they must never share a pipeline name with a concurrently running
  incremental.
- **db paged snapshots** run `plan`, a `Map` over the pages, then `finalize`:
  [examples/db_snapshot.asl.json](../examples/db_snapshot.asl.json).
- **Every invocation is a cold start** by design: the local working directory is dropped
  and state is restored from S3 each time. A warm container behaves exactly like a cold
  one.

## Alarms

- Lambda `Errors` per function.
- api: a log metric filter on `BrokenPageError` separates "vendor sent garbage" from
  other failures. Watch for new objects under `_quarantine/`. In silver, alert on rows
  routed to quarantine.
- db: the EMF metrics `PreflightFailed`, `ConnectorXFailed`, `BackendChanged` per table
  ([db/errors.md](db/errors.md)); watch `NullCursorRows`.

## Terraform

`terraform/modules/api_bronze_source` and `terraform/modules/db_bronze_source` encode
this section (function, IAM derived from the config, schedules, the split-loading loop,
alarms; for db also the VPC security group and the paged-snapshot state machine), and
`terraform/examples/` deploys the sample sources. See
[terraform/README.md](../terraform/README.md).

## Operations

- **Re-run a period:** run a `backfill` window. It is append-only, and silver dedups.
- **Reset a source's cursor:** use a new `pipeline_name`. Its first incremental run
  starts from `initial_value`. Do not delete `_dlt_loads/` objects: silver uses them to
  decide which data files are complete. Never delete bronze data prefixes to do this
  either: dlt also wipes local state when its dataset disappears.
- **Adding a source:** a config JSON in `CONFIG_URI`, a function (image +
  event/schedule), and IAM. For a database, run `{"mode": "validate"}` first.
- **Upgrading dlt** (pinned at 1.32.0 on purpose): the api envelope relies on
  RESTClient internals (the response hook replacing the default `raise_for_status`,
  and every paginator parsing through `response.json()`), and both packages rely on
  dlt's incremental and Arrow behaviour. Re-run the whole test suite, the integration
  suite and both image smoke tests before upgrading.
