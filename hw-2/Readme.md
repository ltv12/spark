###Building and deploy instruction

*  Load [Spark project](https://github.com/ltv12/spark) from gitHub
*  Build project using **SBT** (Scala Build tool)
    * ``` sbt clean package ```
*  Get **spark-hw-2.2.10-1.0.jar** from ***<project_home>/hw-2/target/scala/*** file from local machine to HDP
*  Run jar file using spark-submit :
    * ``` spark-submit --jars /<path_to_jar>/spark-hw-2_2.10-1.0.jar some_arg ```

##### Allowed some arguments from this list :

* **Query1** - Count total number of flights per carrier in 2007
* **Query2** - The total number of flights served in Jun 2007 by NYC
* **Query3** - Find five most busy airports in US during Jun 01 - Aug 31
* **Query4** - Find the carrier who served the biggest number of flights

#####**NOTICE**
This implementation use HiveContext. It means that you have to use this
datasets from [here](http://stat-computing.org/dataexpo/2009/the-data.html)
(load only year 2007) and [here](http://stat-computing.org/dataexpo/2009/supplemental-data.html)
(load Airports and Carrier Codes).
Then put this data into **HDFS** and use **Hive** and use this [scripts](https://github.com/ltv12/hive/tree/master/hive/hw-1/scripts).
with prefix **create_**. Don't forget to add suffix **_orc** to table names in this scripts or use this
[script](https://github.com/ltv12/hive/blob/master/hive/hw-4/scripts/scheme_ORC.sql) to create table with **_orc**
suffix in ORC format.

