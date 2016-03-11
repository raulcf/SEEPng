package uk.ac.imperial.lsds.seep.mdflang;

import java.util.ArrayList;
import java.util.List;

public class Main {
	
	public static void main(String args[]) {
		
		Main m = new Main();
		
		List<Integer> fakeData = new ArrayList<>();
		for(int i = 0; i < 5; i++) {
			fakeData.add(i);
		}
		
		MDFData<Integer> synth = MDF.fromCollection(fakeData);
		
		MDFData<Integer> other = synth.map((Integer a) -> {
			return a + 1;
		});
		
		MDFData<Integer> third = other.map((Integer a) -> {
			return a + 1;
		});
		
		MDFData<Integer> fourth = third.map((Integer a) -> {
			return a;
		}).map((Integer b) -> {
			return b;
		});
		
		MDF.print(fourth);
	
	}
}
