# Terraform

Reusable modules and runnable examples that deploy api_bronze and db_bronze as the
operator docs describe them ([README.md](../README.md) "Deploying on Lambda"). They have
been planned against a mocked AWS provider only; like the rest of the project, they have
**not been applied to a real account yet** (README "Open items" 1).

```
modules/
  bronze_platform/     ECR repositories, bronze bucket, config bucket (CONFIG_URI)
  api_bronze_source/   one API source: config object + function + schedules + alarms
  db_bronze_source/    one database source: same, in a VPC, + optional paged-snapshot state machine
  bronze_function/     internal: what the two source modules share
examples/
  api_bronze/          vendor_orders (split-loading loop), vendor_countries (daily full), vendor_orders_backfill
  db_bronze/           core_banking (incremental loop + daily full of the snapshot table), core_banking_snapshot
```

## What a source module encodes

A source is a config document plus a function. Pass the config as decoded JSON; the
module uploads it to `<CONFIG_URI>/<source_id>.json` and derives from it:

| From the config | Becomes |
|---|---|
| `bucket_url` + `dataset_name` | S3 list/read/write/delete on `<bucket_url>/<dataset_name>/` (data, `_dlt_loads`, `_dlt_pipeline_state`, db `_manifests`) |
| `quarantine_url` (default `bucket_url`) + `pipeline_name` | api: the same on `<quarantine_url>/_quarantine/<pipeline_name>/` |
| `{"secret_id": ...}` under `client.auth`/`client.headers` (api) or `connection` (db) | `secretsmanager:GetSecretValue` on those secrets; add nested ones with `extra_secret_ids` |
| `resources[*].incremental.sorted_ascending` (api) | `loop = true` is rejected unless every resource is sorted |
| `tables[*].name` (db) | one alarm per table for each EMF metric (`PreflightFailed`, `ConnectorXFailed`, `BackendChanged`) |
| `connection.port` (db) | the security group's egress to the database |

The module output `grants` shows what was derived. The S3 action set is a first guess:
confirm it on the first real-S3 run.

Defaults (module variables): 900 s timeout (`timeout_s`), `SAFETY_MARGIN_S` 120
(`safety_margin_s`), reserved concurrency 1 (override with `null` only for a function
whose config has its own `pipeline_name`). Fixed by the modules: container image,
Lambda's own async retries off (a failed run lands nothing and holds the
cursor; the next scheduled run retries), `Errors` alarm, and on api sources a
`BrokenPageError` log-metric alarm. db functions are x86_64 in a VPC; their subnets need a
path to S3 and Secrets Manager (VPC endpoints or NAT).

**Schedules** (`schedules = { name = { expression, event, loop } }`) use EventBridge
Scheduler. `event` defaults to `{"source_id": ...}`. `loop = true` runs the event through
a state machine that re-invokes while `is_empty` is false, capped by
`max_loop_iterations` (reaching it fails the execution, which alarms). Only incremental
events may loop: a full or backfill run returns `is_empty: false` whenever it lands rows.

**Paged snapshots** (db, `snapshot_state_machine = true`): the state machine is
`examples/db_snapshot.asl.json` rendered with the function ARN; `tests/test_terraform_examples.py`
keeps the two identical. Start an execution with
`{"source_id": "...", "table": "...", "page_size": N}`.

## Deploying an example

A function cannot be created before its image exists in ECR, so the first apply is two-phase:

```sh
cd terraform/examples/api_bronze
cp terraform.tfvars.example terraform.tfvars        # edit; uncomment the backend in versions.tf
terraform init
terraform apply -target=module.platform             # ECR + buckets

repo=$(terraform output -raw api_repository_url)
tag=$(git rev-parse --short HEAD)
mise run docker:build -t "$repo:$tag"               # -p linux/amd64 for x86_64 functions
aws ecr get-login-password | docker login --username AWS --password-stdin "${repo%%/*}"
docker push "$repo:$tag"

terraform apply -var image_tag="$tag"               # everything else
```

The db example is the same with `db_repository_url` and `mise run docker:build --db`.
Repositories are immutable-tagged, so every build needs a new tag. Changing a config is a
`terraform apply` (the object changes; the function reads it on the next run). Never
change `pipeline_name`, `dataset_name` or `bucket_url` of a running source: that restarts
it from `initial_value`.

## Checks

```sh
mise run tf            # all of the below
mise run tf:fmt        # terraform fmt -check (--fix rewrites)
mise run tf:validate   # init -backend=false + validate, every module and example
mise run tf:test       # terraform test: plan against a mocked AWS provider, asserts + validation guards
mise run tf:lint       # tflint, terraform + AWS rulesets (terraform/.tflint.hcl)
```

They need the Terraform registry and GitHub (tflint plugin) but no AWS credentials, so
they are not part of the offline `mise run check`.
