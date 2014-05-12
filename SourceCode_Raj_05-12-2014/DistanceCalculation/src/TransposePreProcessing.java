import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;


/**
 * @author Raj
 *
 */
public class TransposePreProcessing {
	
	/**
	 * @param inputPath
	 * @return path of the output file
	 * @throws IOException
	 * Routine to parse the given input dataset into a matrix format
	 * The format is rowindex, columnindex, value
	 */
	public static String dataToMatrixConv(String inputPath) throws IOException {
		/*
		 * Preprocess the input file. The input file is assumed to be of the
		 * form 
		 * 1,2,3,4 
		 * 5,6,7,8 
		 * The preprocessing code will change the input file into 
		 * 1,1,1 
		 * 1,2,2 
		 * 1,3,3 
		 * 1,4,4 etc
		 */
		String outputPath = new File("").getAbsolutePath();
		Date date = new Date();
		outputPath +=date.getSeconds()+date.getMinutes()+date.getHours()+date.getDay();
		System.out.println(outputPath);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputPath));
		BufferedReader bufferedReader = new BufferedReader(new FileReader(inputPath));
		String outputLine = "";
		int rowIndex = 1;
		String outputValues = "";
		while((outputLine = bufferedReader.readLine()) != null) {
			String[] individualValues = outputLine.split(",");
			for (int columnIndex = 1; columnIndex <= individualValues.length; columnIndex++) {
				outputValues+=rowIndex+","+columnIndex+","+individualValues[columnIndex-1];
				bufferedWriter.write(outputValues);
				bufferedWriter.newLine();
				outputValues = "";
			}
			rowIndex += 1;
		}
		bufferedWriter.close();
		bufferedReader.close();
		return outputPath;
	}
	
	public static void main(String[] args) {
		try {
			dataToMatrixConv("/opt/spark/data/test/test2.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
