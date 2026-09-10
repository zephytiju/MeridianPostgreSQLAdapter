# Read-only compatibility with a stored put V1 Resource

PostgreSQL adapter 2.4.0 can read an existing structured store whose stored
ResourceDefinition requires `meridian.structured.put` 1.0.0 while the installed
reader's ResourceDefinition requires 2.0.0. All other definition fields must be
identical, including the exact read contracts, mutation guarantees and limits,
Schema reference, scope, labels, related Resources and extensions.

This is an explicit deployment selection. Ordinary Bindings retain their existing
strict equality checks and read/write behavior. The adapter does not infer
compatibility from a failed fingerprint check, rewrite a ledger, or migrate the
store during startup. Install the reader's dependencies normally; this option
does not permit conflicting Core or Catalog versions in one Python environment.

Add this optional member to `meridian.postgresql.settings.v1`:

```python
settings["readCompatibility"] = {
    "formatVersion": "meridian.postgresql.read-compatibility.v1",
    "resources": [
        {"storedResource": stored.to_dict(), "readerResource": reader.to_dict()}
        for stored, reader in selected_resource_pairs
    ],
}
```

Use the exact full definitions from the selected stored and reader provider
releases in `selected_resource_pairs`. The adapter reconstructs them through the
public Core ResourceDefinition contract and computes both fingerprints. An
arbitrary pair of hash strings, missing fields, unknown fields, or an unsupported
difference is rejected. Every layout in this Binding must have exactly one pair;
only structured data Resources using the put V1 to V2 transition are eligible.
Use a separate Binding for current read/write storage.

The normal layout `resourceFingerprint` and runtime Resource pin select the
**reader** definition. Keep the stored field layout, `schemaFingerprint`, scope
columns, profile, and `requiredPhysicalFingerprint` unchanged. The latter is
mandatory and must select the existing physical plan. Do not compile/apply a new
migration to make the reader's metadata match. Physical verification independently
requires the ledger's exact stored Resource fingerprint, the reader's exact
Resource fingerprint, the common Schema fingerprint and profile, and the actual
table shape. Existing physical drift still fails startup.

The whole compatibility Binding is read-only. The runtime configures pooled
connections with `default_transaction_read_only=on`, verifies that mode during
readiness, and accepts a runtime identity with SELECT on the pinned data tables
and metadata ledger plus database CONNECT and schema USAGE. Existing topology,
TLS, isolation and feature probes remain in force. The adapter still requires the
selected primary endpoint; this feature does not introduce replica routing.

Only read Operations (`get`, `query`, `aggregate`, `search`, `traverse`) marked
read-only may compile. Put, patch, delete, append, claims, Schema publication,
activation, physical migrations and logical import are rejected before SQL.
Marking a mutation read-only does not bypass the guard. Database read-only mode
also rejects writes using a privileged credential. Normal query predicates still
require the original tenant and every configured scope value.

To roll back the reader, close it and restore the prior reader deployment. There
is no store rollback: the reader has not changed its data or metadata. The legacy
writer can restart with its original packages, settings and fingerprints, and
continue its original put behavior. The CI acceptance uses separately installed
1.0.0 writer and candidate reader wheels, checks the unchanged ledger, then
restarts the older writer and exercises its existing update behavior.
