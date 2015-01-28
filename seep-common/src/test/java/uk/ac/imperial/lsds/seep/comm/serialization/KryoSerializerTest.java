package uk.ac.imperial.lsds.seep.comm.serialization;

import java.util.ArrayList;

import org.junit.Test;

public class KryoSerializerTest {

	@Test
	public void test() {
		double[] a = new double[10];
		ArrayList<Double> d = new ArrayList<>();
		
		for(int i = 0; i < 10; i++){
			a[i] = i * 32.4;
			d.add(a[i]);
		}
		
		Serializer<double[]> k = KryoSerializer.getTypedSerializer(a.getClass(), d.getClass());
		
		int rounds = 1000;
		long start = System.currentTimeMillis();
		while(rounds > 0){
			byte[] arraySerialized = k.serialize(a);
			double[] deser = k.deserialize(arraySerialized, double[].class);
			for(int i = 0; i < 10; i++){
				System.out.println("a: "+a[i]+" aser: "+deser[i]);
			}
			rounds--;
		}
		long stop = System.currentTimeMillis();
		System.out.println("Elapsed time: "+(stop-start));
		
	}

}
