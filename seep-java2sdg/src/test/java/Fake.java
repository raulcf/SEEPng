import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.api.SeepProgram;
import uk.ac.imperial.lsds.java2sdg.api.SeepProgramConfiguration;
import uk.ac.imperial.lsds.seep.api.DataOrigin;
import uk.ac.imperial.lsds.seep.api.DataOriginType;
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
		Schema sch = SchemaBuilder.getInstance().newField(Type.INT, "id").build();
		DataOrigin trainSrc = new DataOrigin(DataOriginType.NETWORK, "localhost:5555", SerializerType.NONE, sch, null);
		spc.newWorkflow("train", trainSrc);
		
		// declare test workflow
		Schema sch2 = SchemaBuilder.getInstance().newField(Type.INT, "id").build();
		DataOrigin testSrc = new DataOrigin(DataOriginType.NETWORK, "localhost:5556", SerializerType.NONE, sch2, null);
		DataOrigin testSnk = new DataOrigin(DataOriginType.CONSOLE, "System.out", SerializerType.NONE, null, null);
		spc.newWorkflow("test", testSrc, testSnk);

		return spc;
	}
	
	public double train(){
		iteration = 5;
		List<Double> weights = new ArrayList<Double>();
		for(int i = 0; i < iteration; i++){
			weights.add((double) (i*8));
			@Global
			double gradient = 4*5;
		}
		return weights.get(0);
	}
	
	public void test(float data){
		int a = 0;
		int b = 1;
	}
	
	@Collection
	public void merge(List<Integer> numbers){
		
	}
}
