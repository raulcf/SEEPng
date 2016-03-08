package uk.ac.imperial.lsds.seep.mdflang;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Main {

	public double matchOnInt(int a) {
		return (double)a;
	}
	
	Function<Integer, Double> f = (Integer a) -> {
		return (double)a;
	};
	
	@SuppressWarnings("unchecked")
	static public MDFData<Integer> createMDFDataInts() {
		List<Integer> ints = new ArrayList<>();
		ints.add(1);
		ints.add(2);
		ints.add(3);
		return (MDFData<Integer>) ints;
	}
	
	public static void main(String args[]) {
		Main m = new Main();
		
		MDFData<Integer> ints = Main.createMDFDataInts();
	
	}
}
