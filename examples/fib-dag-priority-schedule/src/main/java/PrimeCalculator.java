import java.math.BigInteger;

/**
 * @author pg1712
 *
 */
public class PrimeCalculator {

	
	public static boolean isPrime(BigInteger n) {
	    BigInteger counter = BigInteger.ONE.add(BigInteger.ONE);
	    boolean isPrime = true;
	    while (counter.compareTo(n) == -1) {
	        if (n.remainder(counter).compareTo(BigInteger.ZERO) == 0) {
	            isPrime = false;
	            break;
	        }
	        counter = counter.add(BigInteger.ONE);
	    }
	    return isPrime;
	}

	public static void main(String[] args) {
		// ExecutorService executorService = Executors.newFixedThreadPool(10);
		// for (int j = 0; j < 10; j++) {
		final int ID = 1;
		// executorService.submit(new Runnable() {
		// public void run() {
		BigInteger number = BigInteger.ONE;
		while (true) {
			System.out.println("Num "+number);
			long start = System.currentTimeMillis();
			System.out.println("Worker: " + ID + " isPrime: " + isPrime(number));
			System.out.println("Took: "+ (System.currentTimeMillis()-start)+"ms");
			number = number.add(BigInteger.ONE);
		}
	}
	// });
	// }
	// }
}
