# elastic-asset-etl-poc
POC for an Elastic asset ETL

To run, copy config/config.sample.json to config/config.json and update the REDACTED parts accordingly.

After running `npm install` you can run the "run.ts" script (or create similar scripts) by running e.g. `npx ts-node run.ts --read=edge --write=local_es_snapshot_ssl`
