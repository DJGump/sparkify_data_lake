# sparkify_data_lake
A data lake and ETL job built with Spark on AWS EMR

### How to run
1. Initialize an EMR cluster as follows:
    * Release: emr-5.20.0
    * Software:
        * Ganglia 3.7.2
        * Spark 2.4.0
        * Zeppelin 0.8.0
    * All other settings as default
    * Note:These instructions assume the source code will be run directly from the master node. Configure EMR with pem file, security group allowing incoming traffic from your ip, and other custom steps to facilitate your workflow of choice. Some options are AWS CLI and bash SSH. 
2. 



