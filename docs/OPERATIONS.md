# Optional deployment interfaces

Machine acquisition, email, UPS access and automatic retention are disabled in the example.
Keep them disabled for the demonstration. Prepare separate ignored local configuration for an
installation; never edit committed templates with live values.

An installation needs an approved read-only endpoint, NodeIds, units, timestamp/latch semantics,
queue settings, server trust, credentials and production-state definition. Values are not inferred
from export headings. Capture gaps require approved history/export reconciliation.

Systemd templates use `/opt/edge-logger`, `/etc/edge-logger` and `/var/lib/edge-logger`.
An administrator must review and install them. The optional poweroff path/service consumes a
durable UPS request; enable it only after validating a trusted utility's power/battery JSON and
physical shutdown behaviour. No vendor pin or register assignments are guessed.

Use `backup-db` for online SQLite backups. Keep backups off the edge computer and monitor disk
growth, clock synchronization, data freshness and delivery state. Storage acknowledgement must
refer to the exact workbook and cycle export before file retention can apply. Canonical cycles
remain in SQLite; physical media and backup testing are still required.

Network-isolation templates are examples for a separately commissioned device. Do not apply
them to a development workstation or treat them as proof of a secure installation.
