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
	
Running the code submitted on 09-11-2014
---------------------------------------
1. After checking out the code, do a maven build by executing the following command in the command line
	mvn clean install
2. The executable jar will be present in the <PATH_TO_CHECKED_OUT_CODE>/target
3. Modify the fuzzyparams.properties based on the cluster configuration and the input data location
4. Execute the following command after spark is started
	java -cp "PATH_TO_EXECUTABLE_JAR:PATH_TOSPARK_JAR" edu.uw.fuzzyroughset.FuzzyRoughset PATH_TO_fuzzyparams.properties