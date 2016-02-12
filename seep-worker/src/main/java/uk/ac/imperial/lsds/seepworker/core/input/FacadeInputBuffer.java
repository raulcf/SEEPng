package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.channels.ReadableByteChannel;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class FacadeInputBuffer implements IBuffer {

	private DataReference dr;
	
	private FacadeInputBuffer(WorkerConfig wc, DataReference dr) {
		this.dr = dr;
	}
	
	public static FacadeInputBuffer makeOneFor(WorkerConfig wc, DataReference dr) {
		return new FacadeInputBuffer(wc, dr);
	}
	
	@Override
	public DataReference getDataReference() {
		return dr;
	}
	
	@Override
	public int readFrom(ReadableByteChannel channel) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] read(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pushData(byte[] data) {
		// TODO Auto-generated method stub
		
	}

}
