###Building and deploy instruction

*  Load [Spark project](https://github.com/ltv12/spark) from gitHub
*  Build project using **SBT** (Scala Build tool): ``` sbt clean package ```
*  Put **spark-hw-1.2.10-1.0.jar** file from hw-1/target/scala-2.10 to HDP
*  Get **UserAgentUtils-1.14.jar** from local maven repository
*  Put **spark-hw-1.2.10-1.0** and **UserAgentUtils-1.14.jar** on HDFS 
    ``` hdfs dfs -copyFromLocal /<local_jars_location> /<hdfs_jar_location> ```
*  Run jar file using spark-submit : 
    ``` spark-submit --jars /<path_to_jar>/UserAgentUtils-1.14.jar /<path_to_jar>/spark-hw-1_2.10-1.0.jar <input_directory_or_file> <output_directory> ```
