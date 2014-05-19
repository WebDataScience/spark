import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * @author Raj
 * 
 */
public class ProximityGraph {

	/**
	 * @param args
	 * Routine for calculating Euclidean distance for the given dataset
	 * Each tuple in the dataset is considered as a point
	 */

	public static void main(String[] args) {
		try {
			/*
			 * Parameters for spark. They have to be entered through command
			 * line
			 */
			String sparkServer = args[0];
			String sparkHome = args[1];
			String filePath = args[2];
			
			//Preprocessing the input data file
			String matrixPath = TransposePreProcessing
			  .dataToMatrixConv(filePath);
			 
			// Set the spark context
			JavaSparkContext sc = new JavaSparkContext(sparkServer,
					"Proximity Graph", sparkHome,
					JavaSparkContext.jarOfClass(ProximityGraph.class));
			// Generate an RDD for the preprocessed input file and cache it
			JavaRDD<String[]> datasetRDD = sc.textFile(matrixPath).map(
					new DataParser());

			// Convert the above RDD into a JavaPairRDD with column index as key
			// and the entire tuple as value
			JavaPairRDD<Integer, String[]> matrixRDD = datasetRDD
					.map(new PairFunction<String[], Integer, String[]>() {

						@Override
						public Tuple2<Integer, String[]> call(String[] arg0)
								throws Exception {

							int point = Integer.parseInt(arg0[1]);
							return new Tuple2<Integer, String[]>(point, arg0);
						}

					});
			datasetRDD = null;

			// Generate a transpose RDD for the above preprocessed file
			JavaRDD<String[]> transposeRDD = sc.textFile(matrixPath).map(
					new TransposeParser());
			// Covert the transpose matrix into a JavaPairRDD with row index as
			// key and the entire tuple as value
			JavaPairRDD<Integer, String[]> transposeMatrixRDD = transposeRDD
					.map(new PairFunction<String[], Integer, String[]>() {

						@Override
						public Tuple2<Integer, String[]> call(String[] arg0)
								throws Exception {
							// TODO Auto-generated method stub
							int point = Integer.parseInt(arg0[0]);
							return new Tuple2<Integer, String[]>(point, arg0);
						}

					});
			transposeRDD = null;

			// Join the original RDD and transpose RDD. Inbuilt method join is
			// used. It joins the RDD's based on key
			JavaPairRDD<Integer, Tuple2<String[], String[]>> jointPairs = matrixRDD
					.join(transposeMatrixRDD);
			matrixRDD = null;
			transposeMatrixRDD = null;

			// Perform the first step for calculating the Euclidean distance in
			// the mapper
			JavaPairRDD<String, Double> intermediateRDD = jointPairs
					.map(new PairFunction<Tuple2<Integer, Tuple2<String[], String[]>>, String, Double>() {

						@Override
						public Tuple2<String, Double> call(
								Tuple2<Integer, Tuple2<String[], String[]>> arg0)
								throws Exception {

							double i = Double.parseDouble(arg0._2._1[2]);
							double j = Double.parseDouble(arg0._2._2[2]);
							double difference = i - j;
							double result = difference * difference;
							double[] key = new double[2];
							key[0] = Double.parseDouble(arg0._2._1[0]);
							key[1] = Double.parseDouble(arg0._2._2[1]);
							return new Tuple2<String, Double>(key[0] + ","
									+ key[1], result);

						}

					});

			// Add all the differences calculated in the mapper based on key
			intermediateRDD = intermediateRDD
					.reduceByKey(new Function2<Double, Double, Double>() {

						@Override
						public Double call(Double arg0, Double arg1)
								throws Exception {
							arg0 += arg1;
							return arg0;
						}

					});

			// Create another RDD which calculates the Square Root of the
			// value from previous RDD. This will give the final distance matrix
			JavaPairRDD<String, Double> distanceRDD = intermediateRDD
					.map(new PairFunction<Tuple2<String, Double>, String, Double>() {

						@Override
						public Tuple2<String, Double> call(
								Tuple2<String, Double> arg0) throws Exception {
							String key = arg0._1;
							double result = Math.sqrt(arg0._2);
							return new Tuple2<String, Double>(key, result);
						}

					});
			// Collect the result in a list
			List distanceRDDList = distanceRDD.collect();
			System.out.println("Done");
			//Uncomment to test the correctness of the above job
			//This will print the result in the console.
			/*Iterator iterator = distanceRDDList.iterator();
			System.out.println("Distance RDD after Reduce");
			while (iterator.hasNext()) {
				Tuple2<String, Double> tuple = (Tuple2<String, Double>) iterator
						.next();
				double r = tuple._2;
				System.out.println(tuple._1 + ":" + r);
			}*/
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}