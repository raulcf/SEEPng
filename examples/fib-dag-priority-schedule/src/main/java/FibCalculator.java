import java.math.BigInteger;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

/**
 * @author pg1712
 *
 */
public class FibCalculator implements SeepTask{
	
	Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "processTime").newField(Type.LONG, "timestamp").build();
	long i = 0;
	
	public static BigInteger fib(BigInteger n) {
	    if (n.compareTo(BigInteger.ONE) == -1 || n.compareTo(BigInteger.ONE) == 0 ) return n;
	    else 
	        return fib(n.subtract(BigInteger.ONE)).add(fib(n.subtract(BigInteger.ONE).subtract(BigInteger.ONE)));
	}
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}


	@Override
	public void processData(ITuple data, API api) {
//		long processTime = data.getLong("processTime");		
		long ts = data.getLong("timestamp");
		
		
		long start = System.currentTimeMillis();
		BigInteger tmp = fib(new BigInteger("35"));
		//assert( tmp.compareTo(new BigInteger("102334155")) == 0 );
		long end = System.currentTimeMillis();
		
		if(i%200 == 0)
			System.out.println("[Fib Computation] :" + (end - start) + " - "+i);
		i++;
		
		byte[] processedData = OTuple.create(schema, new String[]{"processTime", "timestamp"},  new Object[]{(end-start), ts});
		api.send(processedData);	
	}


	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
