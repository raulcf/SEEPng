import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.api.SeepProgram;
import uk.ac.imperial.lsds.java2sdg.api.SeepProgramConfiguration;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.annotations.Partial;
import uk.ac.imperial.lsds.seep.api.annotations.Partitioned;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;

public class Fake implements SeepProgram {

	@Partitioned
	private int iteration;
	@Partial
	private List<Double> weights;
	
	@Override
	public SeepProgramConfiguration configure(){
		
		SeepProgramConfiguration spc = new SeepProgramConfiguration();
		
		// declare train workflow
//		Schema sch = SchemaBuilder.getInstance().newField(Type.INT, "id").build();
//		DataStore trainSrc = new DataStore(sch, DataStoreType.NETWORK);
//		spc.newWorkflow("train()", trainSrc);
		
		// declare test workflow
		Schema sch2 = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
		DataStore netSrc = new DataStore(sch2, DataStoreType.NETWORK);
		DataStore netSnk = new DataStore(sch2, DataStoreType.NETWORK); // TODO: CREATE STATIC SINK INSTEAD
		DataStore fileSnk = new DataStore(sch2, DataStoreType.FILE); // TODO: CREATE STATIC SINK INSTEAD
		
		spc.newWorkflow("train(int userId, long ts, String text)", netSrc, netSnk);
//		spc.newWorkflow("test(int userId, long ts, String text)", netSrc, netSnk);
		return spc;
	}
	
	public void train(int userId, long ts, String text) {
//		iteration = 5;
		userId = userId + 1;
		ts = ts +1l;
		text += "_processed";
		int count =0;
		if( (userId % 100000) == 0){
			System.out.println("ID: "+ userId + " TS: "+ ts + "Txt: "+ text);
		}
//		System.out.println("ID: "+ userId + " TS: "+ ts + "Txt: "+ text);		
		List weights = new ArrayList();
		weights.add(new Double("100"));
//		System.out.println("GOT: " + weights.get(0));
		
		
//		for (int i = 0; i < iteration; i++) {
//			weights.add((double) (i * 8));
//
//			double gradient = 4 * 5;
//		}
//		test(10);
//		return weights.get(0);
	}

//	public void test(int userId, long ts, String text) {
//		userId = userId + 1;
//		ts = ts +1l;
//		long whatever = 0l;
//		int b = 1;
//		float c = ts +10;
//		long ts2 = ts + 1;
//		text = new String(text + "_saying_something");
//	}
//
//	@Collection
//	public void merge(List<Integer> numbers) {
//
//	}
}
