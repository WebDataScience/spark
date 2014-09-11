package edu.uw.fuzzyroughset;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author Raj, Hasan
 *
 */
public class FuzzyIteration extends ParallelIteration{
	
	public FuzzyIteration() {
		super();
	}

	public FuzzyIteration(int start, int end, Object args, String row) {
		this.start = start;
		this.end = end;
		this.args = args;
		this.row = row;
	}

	@Override
	public ParallelIteration getInstance(int start, int end, Object args,
			String row) {
		return new FuzzyIteration(start, end, args, row);
	}
	
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
	
	public static double getDistanceN(double x,double y, double range)
	{
		return Math.abs(x-y);
	}
	
	//Calculate similarity between rows and divide it by range
	public static double getSimilarity(double[] row1, double[] row2, double[] ranges)
	{
		double result = 0;
		int numOfColumns = row1.length-1;
		
		for(int i = 0; i<row1.length-1;i++)
			result+=getDistanceN(row1[i], row2[i], ranges[i]);
		result = numOfColumns - result;
		return result/(row1.length-1);
		
	}

	//Routine to calculate upper and lower approximation
	@Override
	public Object runIteration(int start, int end, Object agrs, String row) {
		Map<String, Object> arg = (Map<String, Object>) agrs;
		ArrayList<double[]> inMemoryRows =  (ArrayList<double[]>) arg.get("InMemoryRows");
		double[] rangeVs = (double[]) arg.get("Ranges");
		double[] row1 = parseLine(row, rangeVs);
		double[] row2 = new double[row1.length];
		double maxs = 0;
		double min = 1;
		double simVal = -1;
		String temp = "";
		for(int i = start; i < end; i++) {
			row2 = inMemoryRows.get(i);
			simVal = getSimilarity(row1, row2, rangeVs);
			min = Math.min(simVal, row2[row2.length-1]);
			maxs = Math.max(maxs, min);
		}
		return ""+maxs;
	}

}
