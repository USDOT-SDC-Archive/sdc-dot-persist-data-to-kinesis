# sdc-dot-metadata-ingest

This repository contains the source code for generating the metadata from an s3 key and indexing into Elasticsearch Service.

There are two primary functions serves the need for two different lambda functions:
* **bucket-handler-lambda** - generates the metadata and indexes into Elasticsearch
* **register-kibana-dashboards** - generates the default datalake visualization dashboards