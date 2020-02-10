# FeaturePipe: A Universal Feature Pipeline for Financial Data

Financial data scientists and algorithmic traders are all working from the same data set: stock market transactions. Despite this, data pipelines are siloed between teams, with each developing their own proprietary tools. FeaturePipe aims to provide an easy to use, general financial data pipeline, by converting raw stock data into machine learning features in real time, and delivering them in an easy to use Python package.

## Features

* Streaming live ticker data up to 10K ticks per second
* Features computed for over 5K stocks, updated every 30 seconds
* Developers can subscribe to as many stocks as they are interested in
* Package also includes simple tools to start building trading algorithms: a built in labeler and simulation module

## How It Works

FeaturePipe's data extraction is handled with Apache Pulsar, a pub/sub messaging system.
Raw data is streamed via Polygon.io, a market data provider, stored in S3 via Pulsar's
"Tiered Storage" system, and queried using Pulsar SQL, a wrapper for Presto, to compute
features.

On the client side, developers install the "featurepipe-client" module, where they can
subscribe to features in real time, query historical features, run simple regressions
against the built in labels, and simulate the results of trading.

An API serves as the middleman for client requests to the Pulsar cluster for security reasons.

## Directory Structure

This repository is organized in the following way:

* `producers` produce_raw_data.py handles the conversion of websocket data into Pulsar-friendly data schemas and streams it. produce_features.py runs simultaneous Presto queries
on the raw data to convert it into useful metrics. Also included are class schemas and helpers.
* `featurepipe-client`  The client tools, including subscription, queries, labeling, and simulation.
* `featurepipe-server`  The API through which clients can communicate with the Pulsar cluster.
* `cluster_setup` Description of the cluster setup, scripts to make installation easier.
