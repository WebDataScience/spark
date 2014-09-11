package edu.uw.fuzzyroughset;

/**
 * @author Raj, Hasan, Gayathri
 *
 */
public class MinAccumulator implements org.apache.spark.AccumulatorParam<double[]>
{

	/* Class to find out the minimum value of each
	 * attribute in the dataset
	 */
	@Override
	public double[] addInPlace(double[] arg0, double[] arg1) {
		double[] result = new double[arg0.length];
		for(int i=0;i<arg0.length;i++)
		{
			result[i] = Math.min(arg0[i],arg1[i]);
		}
		return result;
	}

	@Override
	public double[] zero(double[] arg0) {
		for(int i=0;i<arg0.length;i++)
		{
			arg0[i] = Double.MAX_VALUE;
		}
		return arg0;
	}

	@Override
	public double[] addAccumulator(double[] arg0, double[] arg1) {
		double[] result = new double[arg0.length];
		/*System.out.println("arg0.length="+arg0.length+"; arg1.length="+arg1.length+"; result.length="+result.length);
		System.out.println("arg1:");
		for(int i=0;i<arg1.length;i++)
		{
			System.out.println(arg1[i]);
			//result[i] = Math.min(arg0[i],arg1[i]);
		}
		System.out.println("arg0:");
		for(int i=0;i<arg0.length;i++)
		{
			System.out.println(arg0[i]);
			//result[i] = Math.min(arg0[i],arg1[i]);
		}*/
		
		for(int i=0;i<arg0.length;i++)
		{
			result[i] = Math.min(arg0[i],arg1[i]);
		}
		return result;
	}
	
}

