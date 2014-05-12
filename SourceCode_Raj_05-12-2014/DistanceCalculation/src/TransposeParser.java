import org.apache.spark.api.java.function.Function;

/**
 * @author Raj
 *
 */
class TransposeParser extends Function<String, String[]> {

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.WrappedFunction1#call(java.lang.Object)
	 */
	@Override
	public String[] call(String arg0) throws Exception {
		String[] parts = arg0.split(",");
		String temp = parts[0];
		parts[0] = parts [1];
		parts[1] = temp;
		return parts;
	}
	
}
