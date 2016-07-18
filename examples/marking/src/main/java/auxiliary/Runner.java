package auxiliary;


import general.EsperSingleQueryHandler;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Runner {

	public static void main(String[] args) {

		String query = 
				"Select * " + 
				"From marked " +
				"match_recognize ( " +
						  "partition by well " +
						  "measures A.start_timestamp as timestamp, " +
						  "A.well as well, " +
						  "A.magnitude as aMagnitude, " +
						  "B.magnitude as bMagnitude, " +
						  "C.magnitude as cMagnitude " +
						  "pattern (A B C) " +
						  "define A as ((A.end_timestamp - A.start_timestamp) > 2000), "
						  + "B as ((Math.abs(B.magnitude) > 0.1) AND (B.magnitude > A.magnitude)), " +
						  "C as ((Math.abs(C.magnitude) > 0.1) AND (C.magnitude > B.magnitude)) " +
						") ";

		String[] markedTypeBinding = (
				  "start_timestamp:Long,"
				+ "end_timestamp:Long,"
				+ "well:String,"
				+ "injector:Integer,"
				+ "magnitude:Float,"
				).split(",");

		Schema detectedSchema = SchemaBuilder.getInstance()
				.newField(Type.LONG, "timestamp")
				.newField(Type.SHORTSTRING, "well")
				.newField(Type.FLOAT, "aMagnitude")
				.newField(Type.FLOAT, "bMagnitude")
				.newField(Type.FLOAT, "cMagnitude")
				.build();
				
		EsperSingleQueryHandler op = new EsperSingleQueryHandler(query, "EsperDetection", "marked", markedTypeBinding, detectedSchema);
		op.setUp();
		System.out.println("done");
	}
	
	
	

}
