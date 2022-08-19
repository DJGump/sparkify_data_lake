# sparkify_data_lake
A data lake and ETL job built with Spark on AWS EMR

### How to run
1. Initialize an EMR cluster as follows:
    * Release: emr-5.20.0
    * Software:
        * Ganglia 3.7.2
        * Spark 2.4.0
        * Zeppelin 0.8.0
    * Optional: Select the appropriate EC2 keypair if you are using one
    * All other settings as default
2. SSH into the master node using the script from the 'Connect to the Master Node Using SSH' modal in the Summary tab of the console for the cluster you started. Refer to AWS documentation for assistance with ssh connections
3. Install git: yum install git
4. Install configparser module: pip install configparser
    * Note: The installations in steps 3 and 4 are great candidates for cluster bootstrap actions
5. Run: git clone https://github.com/DJGump/sparkify_data_lake.git
6. Navigate into the repo directory resulting from step 5: cd sparkify_data_lake.git
7. Ensure dl.cfg is configured with your information as follows:
    * AWS_ACCESS_KEY_ID: YOUR_AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY: YOUR_AWS_SECRET_ACCESS_KEY
    * INPUT_PATH: s3a://your/s3/read/location/DATA
      * Udacity input path: s3://udacity-dend/
        * DATA must be a directory containing two folders of json data, named and partitioned as:
            * song_data/\*/\*/\*/\*.json
            * log-data/*.json
    * OUTPUT_PATH: s3a://your/s3/write/location/analytics
        * An S3 path leading to a directory named 'analytics', which will hold the 5 parquet tables output
    * Edit dl.cfg using the nano editor on the master node after cloning, or fork the directory to make changes to dl.cfg and git clone accordingly
8. Run etl.py: spark-submit etl.py