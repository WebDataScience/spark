spark
=====
Running the code through eclipse
--------------------------------
1. Set up the code as a Java project in eclipse
2. Export the project from above step as a jar file
3. Open ProximityGraph.java in eclipse
4. Open the Run Configurations window for ProximityGraph.java and give the following arguments in the arguments tab
	If spark is being run in local mode then the first argument is local
	Path to the spark installation should be the second argument eg. /opt/spark/spark-0.9
	Path to the input file should be the third argument eg. /opt/spark/data/test/test.csv
	