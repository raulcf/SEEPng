package auxiliary;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class CSVToByteStream {

	private static String[] typeBinding = 
					( "timestamp:Long,"
					+ "well:Integer,"
					+ "injector:Integer,"
					+ "BHP:Float,"
					+ "CHKPCT:Float,"
					+ "glRate:Float,"
					+ "MASTERxOPEN:Integer,"
					+ "MASTERxCLOSED:Integer,"
					+ "WINGxOPEN:Integer,"
					+ "WINGxCLOSED:Integer,"
					+ "CHKPCT_2:Float,"
					+ "WINGxOPEN_2:Integer,"
					+ "WINGxCLOSED_2:Integer,"
					+ "flowrate:Float"
					).split(",");

	private static int schemaBytes = 8 + 13 * 4; 

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("############################################");
			System.out.println("Usage: CSVToByteStream <path to input csv file> <path to output file>");
			System.exit(0);
		}
		
		String input  = args[0];
		String output = args[1];

		System.out.println("Converting " + input + " to " + output);
		try (
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), "UTF8")); 
			FileOutputStream out = new FileOutputStream(output);
			) {
			
		    // skip header details in first line
		    reader.readLine();
		    
			String line = null;
			ByteBuffer bb = ByteBuffer.allocate(schemaBytes);
			
		    while ((line = reader.readLine()) != null) { 
				String[] values = line.split(",");
				for (int i = 0; i < values.length; i++) {
					hardCodedCastIntoBB(i, values[i], bb);
				}
	            out.write(bb.array());
	            out.flush();
	            bb.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		System.out.println("Done.");
		
//		System.out.println("Check.");
//	
//		try (FileChannel inChannel = new FileInputStream(output).getChannel()) {
//			
//			MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
//			
//			byte[] tuple = new byte[schemaBytes];
//			
//			while (buffer.hasRemaining()) {
//				buffer.get(tuple);
//				ByteBuffer bb = ByteBuffer.wrap(tuple);
//				System.out.println(bb.getLong()+" "+bb.getInt()+" "+bb.getInt()+" "+bb.getFloat()+" "+bb.getFloat());
//			}
//					
//			inChannel.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
	}
	
	private static void hardCodedCastIntoBB(int i, String value, ByteBuffer bb) {
		switch (typeBinding[i].split(":")[1]) {
		case "Integer":
			bb.putInt(Integer.valueOf(value));
			return;
		case "Long":
			bb.putLong(Long.valueOf(value));
			return;
		case "Float":
			bb.putFloat(Float.valueOf(value));
			return;
		default:
			return;
		}
	}

}
