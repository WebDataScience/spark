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
							// TODO Auto-generated method stub
							int point = Integer.parseInt(arg0[1]);
							return new Tuple2<Integer, String[]>(point, arg0);
						}

					});

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

			// Join the original RDD and transpose RDD. Inbuilt method join is
			// used. It joins the RDD's based on key
			JavaPairRDD<Integer, Tuple2<String[], String[]>> jointPairs = matrixRDD
					.join(transposeMatrixRDD);

			// Perform the first step for calculating the Euclidean distance in
			// the mapper
			JavaPairRDD<String, double[]> intermediateRDD = jointPairs
					.map(new PairFunction<Tuple2<Integer, Tuple2<String[], String[]>>, String, double[]>() {

						@Override
						public Tuple2<String, double[]> call(
								Tuple2<Integer, Tuple2<String[], String[]>> arg0)
								throws Exception {

							double[] result = new double[arg0._2._1.length
									+ arg0._2._2.length];
							result[0] = Double.parseDouble(arg0._2._1[0]);
							result[1] = Double.parseDouble(arg0._2._1[1]);
							result[2] = Double.parseDouble(arg0._2._2[0]);
							result[3] = Double.parseDouble(arg0._2._2[1]);
							double i = Double.parseDouble(arg0._2._1[2]);
							double j = Double.parseDouble(arg0._2._2[2]);
							double difference = i - j;
							result[4] = difference * difference;
							double[] key = new double[2];
							key[0] = result[0];
							key[1] = result[3];
							return new Tuple2<String, double[]>(key[0] + ","
									+ key[1], result);

						}

					});

			// Add all the differences calculated in the mapper based on key
			intermediateRDD = intermediateRDD
					.reduceByKey(new Function2<double[], double[], double[]>() {

						@Override
						public double[] call(double[] arg0, double[] arg1)
								throws Exception {
							arg0[4] += arg1[4];
							return arg0;
						}

					});

			// Create another RDD which calculates the Square Root of the
			// value from previous RDD. This will give the final distance matrix
			JavaPairRDD<String, double[]> distanceRDD = intermediateRDD
					.map(new PairFunction<Tuple2<String, double[]>, String, double[]>() {

						@Override
						public Tuple2<String, double[]> call(
								Tuple2<String, double[]> arg0) throws Exception {
							String key = arg0._1;
							arg0._2[4] = Math.sqrt(arg0._2[4]);
							double[] result = { arg0._2[4] };
							return new Tuple2<String, double[]>(key, result);
						}

					});
			// Print the result
			List distanceRDDList = distanceRDD.collect();
			System.out.println("Done");
			/*Iterator iterator = distanceRDDList.iterator();
			System.out.println("Distance RDD after Reduce");
			while (iterator.hasNext()) {
				Tuple2<String, double[]> tuple = (Tuple2<String, double[]>) iterator
						.next();
				double[] r = tuple._2;
				System.out.println(tuple._1 + ":" + r[0]);
			}*/
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}