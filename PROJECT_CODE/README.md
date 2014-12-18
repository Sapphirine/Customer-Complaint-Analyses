###################################################################
CONTENTS OF target/*.jar
###################################################################
com/
com/bigdata/
com/bigdata/complaintanalysis/
com/bigdata/complaintanalysis/ColumnReducer.class
com/bigdata/complaintanalysis/ClassificationAutomator.class
com/bigdata/complaintanalysis/StripColumn.class
com/bigdata/complaintanalysis/ComplaintsCSVtoSeq.class
com/bigdata/complaintanalysis/StatewiseSorter.class

###################################################################
COMPILATION INSTRUCTIONS
###################################################################

1) Clone the directory and navigate to PROJECT_CODE.
2) Install maven if you don't have it.
3) mvn clean install ( Do this after navigating to PROJECT_CODE folder )
4) The jar file compiled goes into location ./target/*.jar
5) To execute program:

hadoop jar target/Classification-Files-Big-Data-Project-1.0.jar com.bigdata.complaintanalysis.ClassificationAutomator data/Consumer_Complaints.csv

View the sequenced file output on HDFS,

hdfs dfs -ls data/classification/$state_name

eg.

blitzavi89@blitzavi89-Lenovo-Ideapad-Flex-14:~/BigData_Project/PROJECT-FILES$ hdfs dfs -ls data/classification/NY
14/12/10 01:52:16 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 1 items
-rw-r--r--   1 blitzavi89 supergroup     803959 2014-12-10 01:51 data/classification/NY/chunk-0


 
