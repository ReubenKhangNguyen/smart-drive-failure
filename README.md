# SMART Drive Failure

This project stores Backblaze Drive Stats in HDFS, transforms Bronze CSV into Silver Parquet with PySpark, creates leakage-safe seven-day risk labels and Gold analytics, then trains Spark MLlib baselines. FastAPI serves compact Gold JSON exports to a React dashboard; it never reads raw CSV data per request.

The Compose stack includes one NameNode, three DataNodes (replication factor 2), one Spark Master, two Spark Workers, a Spark client, FastAPI and React/Vite. See [quick-start.md](docs/quick-start.md) for commands and evidence collection.
