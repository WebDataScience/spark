package edu.uw.fuzzyroughset;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * @author Raj, Hasan, Gayathri
 *
 */
public class FuzzyRoughset {
	
	//Routine to read the configurations of Spark from the properties file
	public static Properties readParameters(String path)
	{
		Properties prop = new Properties();
		InputStream input = null;
	 
		try {
			input = new FileInputStream(path);
			prop.load(input);
	 
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	//Routine to convert the read line from String to double array
	//For example the read line will be 1,2,3,4
	//The routine would return (1,2,3,4) and divide it by range values
	public static double[] parseLine(String line, double[] ranges)
	{
		String[] attrs = line.split(",");
		double[] attrVals = new double[attrs.length];

		for(int i=0; i<attrVals.length - 1;i++)
		{
			attrVals[i] = Double.parseDouble(attrs[i]) * ranges[i];
		}
		attrVals[attrVals.length - 1] = Double.parseDouble(attrs[attrVals.length - 1]);
		return attrVals;
	}
	
	public static void main(String[] args) {
		String paramsPath = args[0];
		Properties properties = null;
		//Load the spark configurations properties file to the properties object
		properties = readParameters(paramsPath);
		//Set all the spark configurations from the properties to the variables
		String sparkServer = properties.getProperty("spark_server");
		String sparkHome = properties.getProperty("spark_home");
		String jarFilePath = properties.getProperty("jar_path");
		final String filePath = properties.getProperty("data_path");
		String outputPath = properties.getProperty("output_path");
		final String hadoopHome = properties.getProperty("hadoop_home");
		final int numOfPartitions = Integer.parseInt(properties.getProperty("partitions"));
		final int numOfCols = Integer.parseInt(properties.getProperty("columns"));
		final int numOfRows = Integer.parseInt(properties.getProperty("rows"));
		final int numOfThreads = Integer.parseInt(properties.getProperty("threads"));
		String defaultParallelism = properties.getProperty("defaultParallelism");
		String executorMemory = properties.getProperty("executorMemory");
		/*
		 * Preparing the Spark environment
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster(sparkServer);
        conf.setAppName("FuzzyRoughSet");
        
        conf.setJars(new String[] { jarFilePath });
        conf.setSparkHome(sparkHome);
        conf.set("spark.default.parallelism", defaultParallelism);
        conf.set("spark.executor.memory", executorMemory);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        long startTime = System.currentTimeMillis()/1000;
        //Form an RDD from the input file. The variable numOfPartitions is equal to the number of nodes in the cluster
        //So that each node will have one part of the input file
		JavaRDD<String> rdd = sc.textFile(filePath,numOfPartitions);
		double[] minVals = new double[numOfCols];
		double[] maxVals = new double[numOfCols];
		double[] rangesVals = new double[numOfCols];
		//Load the min and max value accumulators
		final Accumulator<double[]> min = sc.accumulator(minVals, new MinAccumulator());
		final Accumulator<double[]> max = sc.accumulator(maxVals, new MaxAccumulator());
		//For each tuple identify min and max by using the accumulators
		rdd.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String arg0) throws Exception {
				String[] attrs = arg0.split(",");
				//System.out.println(attrs.length);
				double[] attrVals = new double[attrs.length];
				//System.out.println(attrVals.length);
				for(int i=0; i<attrVals.length;i++)
					attrVals[i] = Double.parseDouble(attrs[i]);
				min.add(attrVals);
				max.add(attrVals);
				
			}
		});
		minVals = min.value();
		maxVals = max.value();
		//Compute the range and get the reciprocal. This is done as part of optimization as division is more costly than multiplication
		for(int i=0;i<numOfCols;i++)
			rangesVals[i] = 1/(maxVals[i]-minVals[i]);
		final Broadcast<double[]> ranges = sc.broadcast(rangesVals);
		
		JavaRDD<ArrayList<String>> rdd2 = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, ArrayList<String>>(){

			@Override
			public Iterable<ArrayList<String>> call(Iterator<String> arg0)
					throws Exception {
				//This is to read from a file in HDFS
				BufferedReader br = null;
				Path pt = new Path(filePath);
				Configuration conf = new Configuration();
				String confPath = hadoopHome+"/conf/core-site.xml";
				confPath.replace("//", "/");
				conf.addResource(new Path(confPath));
				FileSystem fs = FileSystem.get(conf);
				//Uncomment the following line to read from HDFS
				//br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				//Uncomment the following line to read from file system
				br = new BufferedReader(new FileReader(filePath));
				ArrayList<double[]> inMemoryRows = new ArrayList<double[]>();
				String[] rows = new String[numOfThreads];
				FuzzyIteration iter = new FuzzyIteration();
				double[] rangesVs = ranges.value();
				Map<String,Object> args = new HashMap<String,Object>();
				//Load the rows which are in memory to an array list
				while(arg0.hasNext()) {
					inMemoryRows.add(parseLine(arg0.next(), rangesVs));
				}
				//Put the in memory rows and the range values computed above 
				args.put("InMemoryRows", inMemoryRows);
				args.put("Ranges", rangesVs);
				String line = "";
				ArrayList<String> result = new ArrayList<String>();
				int i = 0, count = 0, difference = 0;
				try{
					
					while((line = br.readLine()) != null) {
						//ExecutorService creates a thread pool
						ExecutorService service = Executors.newFixedThreadPool(numOfThreads);
						//Create an array of parallel iteration so that each thread will have a new object of ParallelIteration
						ParallelIteration[] parallelIteration = new ParallelIteration[numOfThreads];
						while(i < numOfThreads) {
							
							count++;
							difference++;
							if(line != null) {
								rows[i] = line;
								//Once a line is read it will be compared against the other rows in the partition and Upper and lower approximations are calculated
								parallelIteration[i] = iter.getInstance(0, inMemoryRows.size(), args, line);
								service.execute(parallelIteration[i]);
								
							}
							i++;
							if(count >= numOfRows && i < rows.length) {
								difference--;
								rows[i] = null;
							}
							if(i < numOfThreads)
								line = br.readLine();
						}
						service.shutdown();
						service.awaitTermination(100, TimeUnit.DAYS);
						for(i = 0;i<difference;i++) {
							//Get the result of each thread and it to a Arraylist
							result.add((String) parallelIteration[i].getResult());
						}
						i = 0;
						difference = 0;
					}
					
				} catch(Exception e) {
					e.printStackTrace();
				}
				ArrayList<ArrayList<String>> finalResult = new ArrayList<ArrayList<String>>();
				finalResult.add(result);
				return finalResult;
			}
			
		});
		
		ArrayList<String> a = rdd2.reduce(new Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>(){

			@Override
			public ArrayList<String> call(ArrayList<String> arg0,
					ArrayList<String> arg1) throws Exception {
				String result = "";
				String[] args0 = new String[2];
				String[] args1 = new String[2];
				for(int i = 0;i<arg0.size();i++) {
					 args0 = arg0.get(i).split(",");
					 args1 = arg1.get(i).split(",");
					result = ""+(Math.max(Double.parseDouble(args0[0]), Double.parseDouble(args1[0])))+","+(Math.min(Double.parseDouble(args0[1]), Double.parseDouble(args1[1])));
					arg0.set(i, result);
				}
				return arg0;
			}
			
		});
		
		for(int i = 0;i<a.size();i++) {
			System.out.println(i+","+a.get(i));
		}
		
		System.out.println("We are done!");
		sc.stop();
		long stopTime = System.currentTimeMillis()/1000;
        System.out.println("Time elapsed: " + (stopTime - startTime) + " seconds");

	}

}
