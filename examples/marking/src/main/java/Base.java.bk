import general.EsperSingleQueryHandler;
import general.Snk;
import general.Src;

import java.util.ArrayList;
import java.util.List;

import masking.CountRatioEvaluator;
import masking.ThresholdChoose;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;


public class Base implements QueryComposer {

	
//	int[] windowMaskingLengths = {2,3,4,5,6,7,8,9};
	int[] windowMaskingLengths = {4};
//	float[] minMaxChanges  = {1.2f,1.4f,1.6f,1.8f,2.0f,2.2f,2.4f,2.6f};
	float[] minMaxChanges = {2.0f};
	
//	int[] windowMarkingLengths = {2,3,4,5,6,7,8,9};
	int[] windowMarkingLengths = {3};
//	float[] markingEventDiffs  = {0.01f,0.05f,0.1f,0.15f,0.2f,0.25f};
	float[] markingEventDiffs  = {0.05f};

//	int[] durations = {2000,4000,8000,16000,48000,96000,192000,384000};
	int[] durations = {4000};
//	float[] magnitudes  = {0.005f,0.01f,0.02f,0.05f,0.1f,0.15f,0.2f,0.25f};
	float[] magnitudes = {0.01f};
	
//	int scaleFactor = 10;
	int scaleFactor = 1;

	public Base(String[] qParams) {
		
		// input string:
		// windowMaskingLengths minMaxChanges windowMarkingLengths markingEventDiffs durations magnitudes scaleFactor
		// example:
		// 2,3,4,5,6,7,8,9,10 1.2f,1.4f,1.6f,1.8f,2.0f,2.2f,2.4f,2.6f 2,3,4,5,6 2,3,4,5,6,7,8,9 0.01f,0.05f,0.1f,0.15f,0.2f,0.25f 2000,4000,8000,16000,48000,96000,192000,384000 0.005f,0.01f,0.02f,0.05f,0.1f,0.15f,0.2f,0.25f 10

		if (qParams.length == 7) {
		
			String[] tokens = qParams[0].split(",");
			windowMaskingLengths = new int[tokens.length];
			for (int i = 0; i < tokens.length; i++)
				windowMaskingLengths[i] = Integer.valueOf(tokens[i]);
			
			tokens = qParams[1].split(",");
			minMaxChanges = new float[tokens.length];
			for (int i = 0; i < tokens.length; i++)
				minMaxChanges[i] = Float.valueOf(tokens[i]);

			tokens = qParams[2].split(",");
			windowMarkingLengths = new int[tokens.length];
			for (int i = 0; i < tokens.length; i++)
				windowMarkingLengths[i] = Integer.valueOf(tokens[i]);

			tokens = qParams[3].split(",");
			markingEventDiffs = new float[tokens.length];
			for (int i = 0; i < tokens.length; i++)
				markingEventDiffs[i] = Float.valueOf(tokens[i]);
		
			tokens = qParams[4].split(",");
			durations = new int[tokens.length];
			for (int i = 0; i < tokens.length; i++)
				durations[i] = Integer.valueOf(tokens[i]);

			tokens = qParams[5].split(",");
			magnitudes = new float[tokens.length];
			for (int i = 0; i < tokens.length; i++)
				magnitudes[i] = Float.valueOf(tokens[i]);
		
			scaleFactor = Integer.valueOf(qParams[6]);
		}
	}

	
	@Override
	public SeepLogicalQuery compose() {
		
		
		/*
		 * Absolute path to the input data file, passed on to the source
		 */
		//String inPath = "E:\\clair_30m.out";
        String inPath =
"/Users/ra/Development/SEEPng/examples/marking/data/clair_30m.out";
		//String inPath = "/home/matthias/repos/15-MDF-usecase/applications/simple-event-marking-query/data/clair_30m.out";
		int totalNumberOfInputEvents = 324652 * scaleFactor;
		
		/*
		 * The sink can be configured to store the results in a file. If the 
		 * flag is set, the given path is used to write results.
		 */
		boolean storeResults = true;
		//String outPath = "E:\\clair_30m_results.out";
        String outPath =
"/Users/ra/Development/SEEPng/examples/marking/data/clair_30m_results.out";
		//String outPath = "/home/matthias/repos/15-MDF-usecase/applications/simple-event-marking-query/data/clair_30m_results.out";

		/* 
		 * ###########################################################
		 * Schema definitions
		 * ###########################################################
		 */
		Schema inSchema = SchemaBuilder.getInstance()
				.newField(Type.LONG, "timestamp")
				.newField(Type.INT, "well")
				.newField(Type.INT, "injector")
				.newField(Type.FLOAT, "BHP") 
				.newField(Type.FLOAT, "CHKPCT") 
				.newField(Type.FLOAT, "glRate") 
				.newField(Type.INT, "MASTERxOPEN")
				.newField(Type.INT, "MASTERxCLOSED")
				.newField(Type.INT, "WINGxOPEN")
				.newField(Type.INT, "WINGxCLOSED")
				.newField(Type.FLOAT, "CHKPCT_2") 
				.newField(Type.INT, "WINGxOPEN_2")
				.newField(Type.INT, "WINGxCLOSED_2")
				.newField(Type.FLOAT, "flowrate")
				.build();

		String[] inTypeBinding = (
				  "timestamp:Long,"
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

		Schema markedSchema = SchemaBuilder.getInstance()
				.newField(Type.LONG, "start_timestamp")
				.newField(Type.LONG, "end_timestamp")
				.newField(Type.SHORTSTRING, "well")
				.newField(Type.INT, "injector")
				.newField(Type.FLOAT, "magnitude")
				.build();
		
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
		
		/* 
		 * ###########################################################
		 * Queries for masking/marking/detection
		 * 
		 * The queries are actually templates that contain 
		 * parameters $XXX that will later be replaced with 
		 * specific values.
		 * ###########################################################
		 */
		String maskingQueryTemplate = 
				"Select * " + 
				"From input.std:groupwin(well).win:length($WIN) " + 
				"Group by well " + 
				"Having max(BHP) < ($MMC * min(BHP))  "
				;
		
		String markingQueryTemplate = 
			"Select first(timestamp), last(timestamp), well, injector, max(BHP)-min(BHP) as magnitude " + 
			"From masked.std:groupwin(well).win:length($WIN) " + 
			"Group by well " + 
			"Having max(BHP) < ($DIF * min(BHP)) "
			;
		
		String detectionQueryTemplate = 
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
						  "define A as ((A.end_timestamp - A.start_timestamp) > $DUR), "
						  + "B as ((B.magnitude > $MAG) AND (B.magnitude > A.magnitude)), " +
						  "C as ((C.magnitude > $MAG) AND (C.magnitude > B.magnitude)) " +
						") ";
		
		/* 
		 * ###########################################################
		 * Prepare the tasks for masking event data
		 * ###########################################################
		 */
		List<SeepTask> maskingTasks = new ArrayList<>();
		
		for (int windowLength : windowMaskingLengths) {
			for (float minMaxChange : minMaxChanges) {
				String query = maskingQueryTemplate.replace("$WIN", String.valueOf(windowLength)).replace("$MMC", String.valueOf(minMaxChange));
				maskingTasks.add(new EsperSingleQueryHandler(query, "EsperMasking", "input", inTypeBinding, inSchema));
			}
		}

		/* 
		 * ###########################################################
		 * Prepare the tasks for marking events
		 * ###########################################################
		 */
		List<SeepTask> markingTasks = new ArrayList<>();
		
		for (int windowLength : windowMarkingLengths) {
			for (float diff : markingEventDiffs) {
				String query = markingQueryTemplate.replace("$WIN", String.valueOf(windowLength)).replace("$DIF", String.valueOf(diff));
				markingTasks.add(new EsperSingleQueryHandler(query, "EsperMarking", "masked", inTypeBinding, markedSchema));
			}
		}

//		SeepTask markingTask   = new EsperSingleQueryHandler(markingQueryTemplate, "EsperMarking", "masked", inTypeBinding, markedSchema);

		/* 
		 * ###########################################################
		 * Prepare the tasks for detecting complex events
		 * ###########################################################
		 */
	
		List<SeepTask> detectionTasks = new ArrayList<>();
		
		for (int duration : durations) {
			for (float magnitude : magnitudes) {
				String query = detectionQueryTemplate.replace("$DUR", String.valueOf(duration)).replace("$MAG", String.valueOf(magnitude));
				detectionTasks.add(new EsperSingleQueryHandler(query, "EsperDetection", "marked", markedTypeBinding, detectedSchema));
			}
		}

//		SeepTask detectionTask = new EsperSingleQueryHandler(detectionQuery, "EsperDetection", "marked", markedTypeBinding, detectedSchema);

		/* 
		 * ###########################################################
		 * Assemble the MDF
		 * ###########################################################
		 */
		int opId = 0;
		int flowId = 0;
				
		LogicalOperator src          = queryAPI.newStatelessSource(new Src(inPath, scaleFactor), opId++);
		LogicalOperator snk          = queryAPI.newStatelessSink(new Snk(outPath, storeResults), opId++);
		LogicalOperator maskingChoose       = queryAPI.newChooseOperator(new ThresholdChoose(0.2f), opId++);
//		LogicalOperator markingChoose       = queryAPI.newChooseOperator(new MinMaxRangeChoose(scaleFactor), opId++);
//		LogicalOperator detectionChoose     = queryAPI.newChooseOperator(new MaxChoose(), opId++);

		LogicalOperator maskingPro = null;
		for (SeepTask t : maskingTasks) {
			maskingPro  = queryAPI.newStatelessOperator(t, opId++);
			src.connectTo(maskingPro, flowId++, new DataStore(inSchema, DataStoreType.NETWORK));

			LogicalOperator eval = queryAPI.newStatelessOperator(new CountRatioEvaluator(totalNumberOfInputEvents), opId++);
			maskingPro.connectTo(eval, flowId++, new DataStore(inSchema, DataStoreType.NETWORK));
			eval.connectTo(maskingChoose, flowId++, new DataStore(inSchema, DataStoreType.NETWORK));
		}

		maskingChoose.connectTo(snk, flowId++, new DataStore(inSchema, DataStoreType.NETWORK));

//		for (SeepTask t : markingTasks) {
//			LogicalOperator markingPro  = queryAPI.newStatelessOperator(t, opId++);
//			maskingChoose.connectTo(markingPro, flowId++, new DataStore(inSchema, DataStoreType.NETWORK));
//
//			LogicalOperator eval = queryAPI.newStatelessOperator(new CountEvaluator(), opId++);
//			markingPro.connectTo(eval, flowId++, new DataStore(markedSchema, DataStoreType.NETWORK));
//			eval.connectTo(markingChoose, flowId++, new DataStore(markedSchema, DataStoreType.NETWORK));
//		}

//		for (SeepTask t : detectionTasks) {
//			LogicalOperator detectionPro  = queryAPI.newStatelessOperator(t, opId++);
//			markingChoose.connectTo(detectionPro, flowId++, new DataStore(markedSchema, DataStoreType.NETWORK));
//
//			LogicalOperator eval = queryAPI.newStatelessOperator(new CountEvaluator(), opId++);
//			detectionPro.connectTo(eval, flowId++, new DataStore(detectedSchema, DataStoreType.NETWORK));
//			eval.connectTo(markingChoose, flowId++, new DataStore(detectedSchema, DataStoreType.NETWORK));
//		}
//
//		detectionChoose.connectTo(snk, flowId++, new DataStore(detectedSchema, DataStoreType.NETWORK));

		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);

		return slq;
	}

}
