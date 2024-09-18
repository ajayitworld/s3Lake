# s3Lake
Create iceberg lake from S3 using Upsolver CLI

Pre-requites:
1) Install upsolver-cli latest version
2) create a mapping.csv with s3 bucket and table
     Example: this mapping.csv file has two entries
        s3://upsolver-samples/orders/,order_demo
        s3://upsolver-samples/sales_info/,sales_info
3) update the configparam.ini for the following parameters
     MAPPING_FILE_PATH = mapping.csv # leave as is if kept in same path else give relative/full path
     copyFromSource = S3             # leave as is for s3
     GLUE_CATALOG = default_glue_catalog # name of your glue catalog
     DB_NAME = ajay_e2e              # name of your database where tables should be created
COMPUTE_CLUSTER = Default Compute    # name of the compute cluster which would process jobs
JOB_PREFIX = load                    # prefix if any for your job names, change to '' if no prefix
S3_CONNECTION = upsolver_s3_samples  # update with your s3 upsolver connection name
runInterval = 1 HOUR
startFrom = BEGINNING
UPSOLVER_TOKEN = <value>             # generate token from UI Settings page and copy paste here

4) This will create iceberg table for each bucket and create s3 copy job to load the bucket into the table 
 
