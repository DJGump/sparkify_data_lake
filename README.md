# Sparkify Data Lake
An ETL job built with Python and Spark on AWS EMR. This script uses pyspark to process song and music streaming json data into a star-schema datawarehouse of parquet files on S3. This transforms raw system logs into a format structured for analysis. Processing jobs such as these are essential for allowing analysts to swifly generate insight to inform business decisions.

This script assumes you are familair with the AWS console, can create an EMR cluster, and read and write S3.



 

### How to run
1. Initialize an EMR cluster as follows:
    * Release: emr-5.20.0
    * Software:
        * Hadoop 2.8.5
        * Ganglia 3.7.2
        * Spark 2.4.0
        * Zeppelin 0.8.0
    * Optional: Select the appropriate EC2 keypair if you are using one
    * Optional: Move the bootstrap.sh file from the repo to an S3 location your cluster can access, and use this location as your bootstrap path. This will handle some additional installations.
    * All other settings as default
2. Once the cluster is in 'Running' state, ssh into the master node using the script from the 'Connect to the Master Node Using SSH' modal in the Summary tab of the AWS console for your cluster. Refer to AWS documentation for assistance with ssh connections to EMR.
3. If you did not use the bootstrap.sh file:
   * Install git: yum install git
   * Install configparser module: pip install configparser
4. Run: git clone https://github.com/DJGump/sparkify_data_lake.git
5. Navigate into the repo directory resulting from step 4: cd sparkify_data_lake.git
6. Ensure dl.cfg is configured with your information as follows:
    * AWS_ACCESS_KEY_ID: YOUR_AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY: YOUR_AWS_SECRET_ACCESS_KEY
    * INPUT_PATH: s3://your/s3/read/location/DATA
      * Udacity input path: s3://udacity-dend/
      * DATA must be a directory containing two folders of json data, named and partitioned as:
        * DATA/song_data/\*/\*/\*/\*.json
        * DATA/log_data/\*/\*/\*.json
    * OUTPUT_PATH: s3a://your/s3/write/location/ANALYTICS
        * An S3 path leading to a directory named 'ANALYTICS', which will hold the 5 parquet tables being output
    * Edit dl.cfg using the nano editor on the master node after cloning, or fork the repository to make changes to dl.cfg and git clone to the master node accordingly. 
7. Run etl.py: spark-submit etl.py. ~50 minutes w/ three m3.xlarge, ~20 minutes with one m5.xlarge (master) & three c5.2xlarge (workers)*.

### Files
* etl.py - Python script performing the ETL job.
* dl.cfg - configuration file, used to update and pass crednetials and input/output paths easily
* bootstrap.sh - An EMR bootstrap script. Installs git and python module configparser onto the master node
* README.md - Read Me markdown file
* LICENSE - License, MIT
* .gitignore - .gitignore.



 \* The latter configuration was experimented with because logs from the former indicated the job desired more executors. The larger cluster has 6 executors with 4 cores per executor (may vary with auto-allocate). 6*4, plus the 4 cores of the driver, puts the system total at 28 cores, 4 shy of the default EC2 service quota limit of 32 cores. More research needs to be done on non-hardware optimizations.
