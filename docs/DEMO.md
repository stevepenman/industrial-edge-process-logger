# Offline demonstration

Run `python tools/run_demo.py` after installing the project. It creates 120 fictional cycles
and uses a configuration derived from `config/config.example.yaml`. All readings come from
deterministic mathematical functions; they are not samples of a real process.

Outputs in ignored `build/demo/` include a synthetic UTF-16 TSV, canonical SQLite database,
raw JSONL mirrors, validated Excel workbook, compressed cycle export and separate receiving
database. The command verifies matching counts and repeated-ingestion safety. It sends no email,
starts no service, requests no shutdown and makes no device connection.

Use another output directory for an independent run:

```shell
python tools/run_demo.py --output build/second-demo --rows 500
```

The workbook includes Summary, Temperature Summary, Temperature Trends, Process Trends,
Events - Exceptions, Active Raw Data, Constant Signals and Source Manifest, plus chart tabs
when relevant. Fictional job identity, a declared two-cavity mould and per-cycle reject quantities
demonstrate the distinction between cycles and physical parts.

`python tools/opcua_simulator.py` starts the optional loopback-only integration simulator.
Its writes are confined to that development server. The simulator is excluded from the installed
logger package and must not be connected to a production device.
