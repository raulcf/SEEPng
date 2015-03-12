package uk.ac.imperial.lsds.seep.integration.performance.microbenchmarks;

public class NanoVSTimePerformance {

	public static void main(String args[]){
		
		long startN = System.currentTimeMillis();
		nano(500000000);
		long stopN = System.currentTimeMillis();
		
		long startC = System.currentTimeMillis();
		current(500000000);
		long stopC = System.currentTimeMillis();
		
		System.out.println("currentTimeMillis: "+(stopN - startN));
		System.out.println("nanoTime: "+(stopC - startC));
		
	}
	
	private static void nano(int iters){
		int a = 0;
		long b = 0;
		for(int i = 0; i < iters; i++){
			a++;
			b = System.nanoTime();
		}
	}
	
	private static void current(int iters){
		int a = 0;
		long b = 0;
		for(int i = 0; i < iters; i++){
			a++;
			b = System.currentTimeMillis();
		}
	}
	
}
