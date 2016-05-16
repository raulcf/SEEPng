package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Utils {

	public static void writeToDOTFile(String graphRepr, String filename) {
		File output = new File(filename+".dot");
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(output));
			bw.write(graphRepr);
			bw.close();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
