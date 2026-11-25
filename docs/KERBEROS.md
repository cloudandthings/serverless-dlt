# Kerberos for db_bronze

Status: **not implemented, not tested** (DESIGN-db.md D11). This guide records the research so a project that
needs Kerberos can start from it. Everything below is desk research from driver documentation and package
inspection. No item has been verified against a real KDC unless it says so. Nothing here is a tested result.

## What an operator sees today

The `auth` schema accepts:

```json
{"type": "kerberos", "principal": "...", "keytab": {"secret_id": "..."}, "krb5_conf": "..."}
```

It parses, then at run start the run fails with `NotImplementedError` (Lambda `errorType`
`NotImplementedError`). The message points to `docs/KERBEROS.md`. Nothing lands and the cursor holds.
Only `{"type": "password"}` is implemented.

## Per engine

| Engine | Backend / driver | Kerberos | Keywords and SPN |
|---|---|---|---|
| MSSQL | ConnectorX (bundles MIT krb5); or `pyodbc` + msodbcsql18 + unixODBC | Yes, on both backends. ConnectorX supports Kerberos on MSSQL only. | `Trusted_Connection=yes` (ConnectorX: `trusted_connection=true`). SPN `MSSQLSvc/<fqdn>:<port>`. The ODBC driver does not renew tickets. `ActiveDirectoryIntegrated` is Entra ID, not on-premises AD. |
| Postgres | `psycopg[binary]` 3.3.x (libpq built `--with-gssapi`). ConnectorX: no (tokio-postgres has no GSSAPI). | Traditional driver only | `krbsrvname` defaults to `postgres`, so the SPN is `postgres/<fqdn>`. |
| MySQL | `PyMySQL`. ConnectorX: no (mysql crate has no GSSAPI). | No | Needs MySQL Enterprise on the server plus mysql-connector-python with its `gssapi` extra, which this project does not use. |
| Oracle | `oracledb` thick mode + Instant Client | Thick mode only. Thin mode has no Kerberos. ConnectorX: plausible through OCI external auth, untested. | `sqlnet.ora` with `SQLNET.AUTHENTICATION_SERVICES=(KERBEROS5)`. |
| Db2 | `ibm_db` + `ibm_db_sa` | Bundled clidriver ships the IBMkrb5 plugin. ConnectorX does not support Db2. | Wheels exist for x86_64 only, no aarch64. |

ConnectorX with `auth.type: kerberos` on any engine except MSSQL must stay rejected. Oracle stays rejected
until the Oracle spike below proves otherwise.

## Getting a ticket

### Option A: keytab plus `kinit`

1. Store the keytab in Secrets Manager (binary secret).
2. On each invocation, write it to a `/tmp` file (mode 0600) and the `krb5.conf` to `/tmp/krb5.conf`.
3. Run `kinit -kt /tmp/<keytab> <principal>` into a FILE ccache (`KRB5CCNAME=FILE:/tmp/krb5cc`).
4. Check the ticket on **every** invocation (`klist -s`) and re-run `kinit` when it is missing or near expiry.
   Warm containers outlive the roughly 10-hour AD ticket lifetime, so a once-per-cold-start `kinit` will
   eventually fail.

### Option B: `KRB5_CLIENT_KTNAME`

Point `KRB5_CLIENT_KTNAME` at the keytab file and let MIT GSSAPI fetch and renew tickets itself, with no
`kinit` step. Open: whether `ibm_db` and the ConnectorX-bundled krb5 honour this variable (see spikes).

## Lambda gotchas

- The KEYRING ccache may not work in Lambda. Use `FILE:` under `/tmp`, the only writable path.
- Set `rdns = false` and `udp_preference_limit = 1` in `krb5.conf` (avoids reverse-DNS canonicalisation
  and UDP fragmentation problems).
- The VPC DNS must resolve the KDCs (and the database hosts by FQDN, since the SPN is built from the FQDN).

## Enabling the image block

`docker/Dockerfile.db` carries a commented-out block: `krb5-libs`, `krb5-workstation`, and
`ENV KRB5_CONFIG=/tmp/krb5.conf KRB5CCNAME=FILE:/tmp/krb5cc`. Uncomment it, rebuild
(`mise run docker:build --db`), then implement the ticket step in the run-start path where the
`NotImplementedError` is raised today. The ConnectorX wheel bundles its own MIT krb5, so the OS packages
matter for `kinit` and the ODBC/libpq/OCI paths.

## Open spikes (all open, none done)

1. ConnectorX MSSQL against on-premises AD.
2. Oracle Kerberos through OCI external auth.
3. Whether `ibm_db` and the ConnectorX-bundled krb5 honour `KRB5_CLIENT_KTNAME`.
4. Whether Instant Client Basic includes the Kerberos adapter.
