Building and deploy instruction

1. load project from git hub
2. package project to jar file
 Tips: sbt clean package
3. put spark-hw-1.2.10-1.0 jar file from hw-1/target/scala-2.10 to HDP
4. Get UserAgentUtils-1.14.jar from local maven repository
5. Put spark-hw-1.2.10-1.0 and UserAgentUtils-1.14.jar on HDFS
Tips: hdfs dfs -copyFromLocal /<local_jars_location> /<hdfs_jar_location>
6. Run jar file using spark-submit
Tips:
spark-submit --jars /<path_to_jar>/UserAgentUtils-1.14.jar /<path_to_jar>/spark-hw-1_2.10-1.0.jar <input_directory_or_file> <output_directory>
