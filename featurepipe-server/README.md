# FeaturePipe-Server

FeaturePipe Server is the API through which clients can query features and access
other useful tools for building financial algorithms. It is built with Flask.

It is intended to be a middleman so that clients cannot directly access the Pulsar
Cluster or other nodes on the VPC; it is configured to receive all traffic, and
it is permitted to communicate with other nodes.

To run: tmux --> python query_api.py.

Only calls to port 80 with an existing path will be received.
