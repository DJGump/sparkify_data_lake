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
2. SSH into the master node using the script from the 'Connect to the Master Node Using SSH' modal in the Summary tab of the console for the cluster you started
3. Install git: yum install git
    * This may require root permission
4. Run: git clone https://github.com/DJGump/sparkify_data_lake.git



