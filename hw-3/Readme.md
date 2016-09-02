###Building and deploy instruction

*  Load [Spark project](https://github.com/ltv12/spark) from gitHub
* Build project using **SBT** (Scala Build tool)
    * To build only hw3 project use ```sbt project hw3```
    * ``` sbt clean package ```
*  Get **spark-hw-3.2.10-1.0.jar** from ***<project_home>/hw-3/target/scala/*** file from local machine to HDP
*  Run jar file using spark-submit :
    * ``` spark-submit --packages com.databricks:spark-csv_2.10:1.4.0 /<path_to_jar>/spark-hw-3_2.10-1.0.jar some_arg ```

##### As argument allowed :

* **input_dir** - Directory with data (*Example: /tmp/source*)
* **output_dir** - Directory where will result placed (*Example: /tmp/target*)

#####**NOTICE**
Input directory should contains unpacked archive [session.dataset.zip](https://drive.google.com/drive/folders/0B4eU5TenoBPjSzdRb3VLajNvbTQ)
Output directory contains data that can be used to get **ROC** curve.