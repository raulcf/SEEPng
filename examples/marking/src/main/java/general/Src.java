package general;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;


public class Src implements Source {
	
	public String dataPath;
	
	//private final Logger LOG = LoggerFactory.getLogger(Src.class.getName());
	
	private int schemaBytes = 8 + 13 * 4; 
		
	private int scaleFactor = 10;

	public Src() {
		
	}

	public Src(String dataPath, int scaleFactor) {
		this.dataPath = dataPath;
		this.scaleFactor = scaleFactor;
	}

	@Override
	public void setUp() {
	}

	@Override
	public void processData(ITuple data, API api) {

		//LOG.debug("Setup node for path: {}", this.dataPath);
        System.out.println("Setup node for path: " + this.dataPath);
		
		try (FileChannel inChannel = new FileInputStream(this.dataPath).getChannel()) {
			
			MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
			
			byte[] tuple = new byte[schemaBytes];
			
			int count = 0;
			for (int i = 0; i < scaleFactor; i++) {
				while (buffer.hasRemaining()) {
					buffer.get(tuple);
					api.send(tuple);
					count++;
				}
			}
			//LOG.debug("Source sent {} tuples in {} round(s)", count, scaleFactor);
			System.out.println("Source sent "+count+" tuples in "+scaleFactor+" round(s)");
					
			inChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void close() {
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
	}

}
