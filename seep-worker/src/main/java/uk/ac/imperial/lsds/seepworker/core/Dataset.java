package uk.ac.imperial.lsds.seepworker.core;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;

public class Dataset implements IBuffer, OBuffer {

	private DataReference dataReference;
	public ByteBuffer buffer = ByteBuffer.allocate(8092);

	public Dataset(DataReference dataReference) {
		this.dataReference = dataReference;
	}
	
	/**
	 * IBuffer
	 */
	
	@Override
	public void readFrom(ReadableByteChannel channel) {
		// TODO Auto-generated method stub
		
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
	
	/**
	 * OBuffer
	 */

	@Override
	public void setEventAPI(EventAPI eAPI) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public EventAPI getEventAPI() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int id() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DataReference getDataReference() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean drainTo(WritableByteChannel channel) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean write(byte[] data) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean readyToWrite() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
