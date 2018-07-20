# sdc-dot-persist-data-to-kinesis

This repository contains the source code for persisting the curated data in Kinesis data stream

There are two primary functions serves the need for two different lambda functions:
* **kinesis_event_lambda_handler** - pushes the curated data to Kinesis data stream
