# stedi-analytics-data
AWS Glue-based ELT- and SQL- processing scripts for Stedi Human Balance Analytics' Accelerometer data.
The files in this project are PySpark-based ELT scripts for reading IoT and time-series data from Stedi's smart rehabilitation devices.
They are meant to show how to implement a simple data-cleaning and staging platform within AWS, which always starts with protecting the privacy of users.

## breakdown
The files in this project are more for reference for those looking to better understand how to implement simple data staging pipelines in AWS Glue from scratch.

The files under `glue-jobs` are each PySpark scripts generated via the drag-and-drop interface offered within AWS Glue Studio's job creation GUI.

Likewise, the DDL scripts under `glue-tables` were generated within AWS Athena for tables created via Glue Studio's Data Catalog -> Glue Table creation interface.

The entire project necessitates the creation of an IAM role with access to Glue, S3, and with a custom policy enabling cross-bucket access between STEDI Analytics' public bucket and whatever bucket(s) you've created as landing platforms.
I did this via AWS' built-in Cloudshell CLI, and instructions for simple S3 bucket and IAM role/policy creation within Cloudshell can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-services-s3.html) and [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-services-iam.html).

The flow is as follows:

1) Customer data is read from Stedi's customer portal (website) and filtered into a trusted zone where only those customers who have opted in to sharing data with research is collected in `customer_landing_to_trusted.py`.
2) Accelerometer data from 'trusted zone' customers (following their opt-in date) is read into its own trusted zone in `accelerometer_landing_to_trusted.py`.
3) Only those customers who have both opted-in to share their data AND who possess usable data make it into the 'curated zone' via `customer_trusted_to_curated`.
4) IoT streaming data is read into its own trusted zone according to the same principle via `iot_raw_to_trusted.py`.
5) Finally, IoT (positioning on the balance device) data is aggregated alongside Accelerometer (electrodes placed on the body) vis a vis timestamps in `aggregate_iot_and_accelerometer_readings` for further analysis by Stedi's scientists.
