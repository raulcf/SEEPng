import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.annotations.Collection;
import uk.ac.imperial.lsds.seep.api.annotations.Global;
import uk.ac.imperial.lsds.seep.api.annotations.NetworkSource;
import uk.ac.imperial.lsds.seep.api.annotations.Partial;
import uk.ac.imperial.lsds.seep.api.annotations.Partitioned;

public class Fake {

	@Partitioned
	private int iteration;
	
	@Partial
	private List<Double> weights;
	
	public void main(){
		int[] data = new int[10];
		train();
		float datatotest = 5;
		test(datatotest);
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
	
	public void test(@Collection float data){
		int a = 0;
		@NetworkSource(endpoint="kafka://localhost:5555")
		int b = 1;
	}
}
