# FeaturePipe-Client

FeaturePipe Client is the package users can install in order to easily access
real time features and machine learning tools.

To install, simply insert this directory into your project root and import client.py.

# Methods

* `get_features` Args: symbol, start_date, end_date. Returns: a Pandas DataFrame of
the features for the specified date.

# Roadmap

* `get_labels` Args: a price time series. Returns: a corresponding array of labels.
* `regress_against`  Args: a DF of features and a labeled series. Returns: the regression results.
* `train`  Args: a DF of features and a labeled series. Returns: a trained regression.
* `simulate`  Args: regression results and price series. Returns: wallet results.
* `run`  Args: trained regression and symbol. Runs indefinitely and signals when buy/sell threshold is breached.
