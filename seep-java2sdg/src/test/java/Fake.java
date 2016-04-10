import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.api.SeepProgram;
import uk.ac.imperial.lsds.java2sdg.api.SeepProgramConfiguration;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.annotations.Collection;
import uk.ac.imperial.lsds.seep.api.annotations.Global;
import uk.ac.imperial.lsds.seep.api.annotations.Partial;
import uk.ac.imperial.lsds.seep.api.annotations.Partitioned;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;

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
		DataStore testSrc = new DataStore(sch2, DataStoreType.NETWORK);
		DataStore testSnk = new DataStore(sch2, DataStoreType.FILE); // TODO: CREATE STATIC SINK INSTEAD
		spc.newWorkflow("test(float data)", testSrc, testSnk); // input and output schema are the same

		return spc;
	}
	
//	public double train(){
//		iteration = 5;
//		List<Double> weights = new ArrayList<Double>();
//		for(int i = 0; i < iteration; i++){
//			weights.add((double) (i*8));
//			@Global
//			double gradient = 4*5;
//		}
//		return weights.get(0);
//	}
	
	public void test(float data){
		int userId = 0; long ts = 0;  String text = "" ;
		long whatever = 0l;
		int b = 1; long ts2 =ts +1; String newText = text + "_saying_something";
	}
	
	@Collection
	public void merge(List<Integer> numbers){
		
	}
}
