package edu.uw.fuzzyroughset;

/**
 * @author Raj, Hasan, Gayathri
 *
 */
public class MaxAccumulator implements org.apache.spark.AccumulatorParam<double[]>
{
	/* Class to find out the maximum value of each
	 * attribute in the dataset
	 */

	@Override
	public double[] addInPlace(double[] arg0, double[] arg1) {
		double[] result = new double[arg0.length];
		for(int i=0;i<arg0.length;i++)
		{
			result[i] = Math.max(arg0[i],arg1[i]);
		}
		return result;
	}

	@Override
	public double[] zero(double[] arg0) {
		for(int i=0;i<arg0.length;i++)
		{
			arg0[i] = 0;//Double.MAX_VALUE;
		}
		return arg0;
	}

	@Override
	public double[] addAccumulator(double[] arg0, double[] arg1) {
		double[] result = new double[arg0.length];
		for(int i=0;i<arg0.length;i++)
		{
			result[i] = Math.max(arg0[i],arg1[i]);
		}
		return result;
	}
	
}

