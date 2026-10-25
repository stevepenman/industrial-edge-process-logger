# CV and interview notes

## Suggested project entry

**Industrial Edge Process Logger | Python, OPC UA, SQLite, Linux / Raspberry Pi**

Designed and implemented a read-only industrial telemetry pipeline with timestamp-aware cycle
assembly, durable SQLite records, restart recovery, validated Excel reporting and a persistent
email outbox. Built a synthetic demonstration and fault-injection tests for data quality,
duplicate protection, interrupted imports, reconnects and storage acknowledgement.

Use this wording only for work you can personally explain and are authorised to describe.
Add the repository link after publication. Do not add employer/client names, real production
metrics, deployment claims, savings or uptime percentages without independently cleared evidence.

## Interview walkthrough

1. Explain source timestamps versus edge-device receive timestamps.
2. Show how out-of-order notifications can mix cycles, and how matching prevents that.
3. Explain the database-before-mirror durability order and the interruption test.
4. Demonstrate repeated imports and a resetting cycle counter.
5. Explain why SMTP acceptance cannot prove durable storage or justify deletion.
6. Distinguish a bad-cycle indicator from a physical reject quantity.
7. Describe what still needs physical commissioning despite software tests.

Run the synthetic demo, inspect its workbook and receiving database, and explain one fault test.
Discuss the tradeoff of stronger durability versus storage and write overhead.
