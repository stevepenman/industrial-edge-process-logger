# Architecture and design decisions

## Data contract

`ProcessRecord` retains machine/run/cycle identity, source and receive timestamps, signal values
with quality and units, and optional production context. A resetting cycle counter is not a
global key: a run identity or UTC source timestamp forms part of duplicate protection.

## Acquisition

The production provider exposes reading, browsing, subscriptions and stop operations. It has
no machine-write or control interface. The development simulator is separate from the installed
package and writes only to its own local server.

Cycle-synchronous readings match the completed-cycle source timestamp. Notifications may arrive
in any order; the provider waits for matching values or records explicit Missing readings.
Independently sampled signals need a declared freshness window. SDK severity methods interpret
Bad and Uncertain status codes. Queue overflow remains visible, reconnects clear cached values,
and a refreshed counter timestamp is not a new physical cycle.

## Durability

SQLite uses WAL, full synchronization, foreign keys and transactional counters. Canonical cycle
payloads commit before their JSONL mirrors. Archive segments are registered before use. Recovery
rebuilds incomplete mirrors, verifies hashes and refuses unexplained count discrepancies.
Each SQLite connection stays on its creating thread. OS locks prevent simultaneous acquisition
and imports from sharing active archive state.

## Reporting and delivery

An hour, calendar or record-count boundary seals a period and opens its successor. Reporting
and SMTP run outside the acquisition loop. Workbooks are validated, checksummed and atomically
promoted; identifiers are written as literal text rather than formulas.

A compressed JSONL export carries stable record IDs. The receiver ingests the entire export
transactionally, deduplicates IDs and refuses conflicting payloads. The email outbox persists
attempt states, safe retry delays and ambiguous outcomes.

Retention requires successful email, a storage receipt, age, complete imports, matching hashes
and managed paths. Canonical records remain in SQLite. This trades additional storage/write
activity for recoverability; capacity planning and backups remain necessary.

## Verification

Tests use constructed records, temporary directories and a loopback-only simulator. Fault tests
exercise interruption between durability boundaries, repeated imports, counter/run changes,
quality problems and uncertain delivery. No real device, credential or plant dataset is required.
Software checks complement physical commissioning; they do not establish machine or UPS behaviour.
