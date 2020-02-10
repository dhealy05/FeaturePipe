Installing a Pulsar cluster
=======

Installation guide at https://pulsar.apache.org/docs/en/2.4.2/deploy-bare-metal/

VPC setup
----
The VPC setup consists of 3 node ZooKeeper ensemble at `10.0.0.4` to `10.0.0.6`

BookKeepers/Brokers at `10.0.0.7` to `10.0.0.9`

Pulsar SQL/Presto workers at `10.0.0.10` to `10.0.0.12`

Pulsar Producers, streaming data from the Polygon API, and querying data from
Presto, at `10.0.0.13`

An API which connects to the cluster but allows traffic from outside the VPC at `10.0.0.14`

Follow the guide above to launch your cluster; "download_all_connectors.sh" makes
it easier to download connectors.
