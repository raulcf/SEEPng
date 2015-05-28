package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class InputAdapterFactory {

	final static private Logger LOG = LoggerFactory.getLogger(IOComm.class.getName());
	
	private static List<InputAdapter> buildInputAdapterOfTypeNetworkForOps(WorkerConfig wc, int streamId, Set<DataReference> drefs, ConnectionType connectionType){
		List<InputAdapter> ias = new ArrayList<>();
		short cType = connectionType.ofType();
		Schema expectedSchema = drefs.iterator().next().getDataStore().getSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			// one-queue-per-conn, one-single-queue, etc.
			LOG.info("Creating inputAdapter for upstream streamId: {} of type {}", streamId, "ONE_AT_A_TIME");
			InputAdapter ia = null;
			for(DataReference dref : drefs) {
				int id = dref.getId();
				ia = new NetworkDataStream(wc, id, streamId, expectedSchema);
				ias.add(ia);
			}
		}
		else if(cType == ConnectionType.UPSTREAM_SYNC_BARRIER.ofType()){
			// one barrier for all connections within the same barrier
			LOG.info("Creating NETWORK inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.UPSTREAM_SYNC_BARRIER.withName());
			InputAdapter ia = new NetworkBarrier(wc, streamId, expectedSchema, drefs);
			ias.add(ia);
		}
		return ias;
	}
	
	private static List<InputAdapter> buildInputAdapterOfTypeFileForOps(WorkerConfig wc, int streamId, Set<DataReference> drefs, ConnectionType connectionType){
		List<InputAdapter> ias = new ArrayList<>();
		short cType = connectionType.ofType();
		Schema expectedSchema = drefs.iterator().next().getDataStore().getSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			LOG.info("Creating FILE inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.ONE_AT_A_TIME.withName());
			for(DataReference dref : drefs) {
				int id = dref.getId();
				InputAdapter ia = new NetworkDataStream(wc, id, streamId, expectedSchema);
				ias.add(ia);
			}
		}
		return ias;
	}
	
	private static List<InputAdapter> buildInputAdapterOfTypeKafkaForOps(WorkerConfig wc, int streamId, Set<DataReference> drefs, ConnectionType connectionType){
		List<InputAdapter> ias = new ArrayList<>();
		short cType = connectionType.ofType();
		Schema expectedSchema = drefs.iterator().next().getDataStore().getSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			LOG.info("Creating KAFKA inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.ONE_AT_A_TIME.withName());
			for(DataReference dref : drefs) {
				int id = dref.getId();
				InputAdapter ia = new NetworkDataStream(wc, id, streamId, expectedSchema);
				ias.add(ia);
			}
		}
		return ias;
	}

	public static List<InputAdapter> buildInputAdapterForStreamId(WorkerConfig wc, int streamId, Set<DataReference> drefs, ConnectionType connectionType) {
		DataStoreType type = drefs.iterator().next().getDataStore().type();
		List<InputAdapter> ias = new ArrayList<>();
		if(type.equals(DataStoreType.NETWORK)){
			ias = buildInputAdapterOfTypeNetworkForOps(wc, streamId, drefs, connectionType);
		}
		else if(type.equals(DataStoreType.FILE)){
			ias = buildInputAdapterOfTypeFileForOps(wc, streamId, drefs, connectionType);
		}
		else if(type.equals(DataStoreType.KAFKA)){
			ias = buildInputAdapterOfTypeKafkaForOps(wc, streamId, drefs, connectionType);
		}
		return ias;
	}
	
}
