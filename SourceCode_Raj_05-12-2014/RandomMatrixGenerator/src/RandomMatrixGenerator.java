import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

/**
 * @author Raj
 * 
 */
public class RandomMatrixGenerator {
	/**
	 * Routine to generate two random matrices and write it into an output file
	 * Modify the for loop limits to change the dimensions of the matrices i
	 * represents row and j represents column of a matrix
	 * 
	 * @throws IOException
	 */
	public static void generateRandomMatrix() throws IOException {
		System.out
				.println("Enter the output file location with file extension. For example C:/assignment/Output.properties");
		BufferedReader outputFileBuffer = new BufferedReader(
				new InputStreamReader(System.in));
		String outputFilePath = outputFileBuffer.readLine();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
				outputFilePath));
		String output = "";
		int randomNumber = 0;
		Random generator = new Random();
		for (int i = 0; i <= 100; i++) {
			for (int j = 0; j <= 100; j++) {
				randomNumber = generator.nextInt(Integer.MAX_VALUE);
				output += randomNumber;
				if(j<100)
					output += ",";
				
			}
			
			bufferedWriter.write(output);
			bufferedWriter.newLine();
			output = "";
		}
		
		bufferedWriter.close();
		outputFileBuffer.close();
		System.out.println("Matrix Generation Completed");
	}

	public static void main(String[] args) {
		try {
			generateRandomMatrix();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}