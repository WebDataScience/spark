package edu.uw.fuzzyroughset;

/**
 * @author Raj, Hasan
 *
 */
public abstract class ParallelIteration implements Runnable{
	
	int start;
	int end;
	Object args;
	String row;
	Object result;
	
	public ParallelIteration()
	{
	}
	
	public ParallelIteration(int start, int end, Object args, String row) {
		this.start = start;
		this.end = end;
		this.args = args;
		this.row = row;
		
	}
	

	public Object getResult() {
		return result;
	}

	@Override
	public void run() {
		result = runIteration(start, end, args, row);
	}

	public abstract ParallelIteration getInstance(int start, int end, Object args, String row);
	
	public abstract Object runIteration(int start, int end, Object agrs, String row);

}
