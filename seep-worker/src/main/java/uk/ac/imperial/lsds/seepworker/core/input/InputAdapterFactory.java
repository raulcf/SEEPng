package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.HdfsInputAdapter;
import uk.ac.imperial.lsds.seepcontrib.kafka.comm.KafkaDataStream;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class InputAdapterFactory {

	final static private Logger LOG = LoggerFactory.getLogger(IOComm.class.getName());
	
	public static List<InputAdapter> buildInputAdapterOfTypeNetworkForOps(WorkerConfig wc, int streamId, List<UpstreamConnection> upc){
		List<InputAdapter> ias = new ArrayList<>();
		short cType = upc.get(0).getConnectionType().ofType();
		Schema expectedSchema = upc.get(0).getExpectedSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			// one-queue-per-conn, one-single-queue, etc.
			LOG.info("Creating inputAdapter for upstream streamId: {} of type {}", streamId, "ONE_AT_A_TIME");
			InputAdapter ia = null;
			for(UpstreamConnection uc : upc){
				int opId = uc.getUpstreamOperator().getOperatorId();
				ia = new NetworkDataStream(wc, opId, streamId, expectedSchema);
				ias.add(ia);
			}
		}
		else if(cType == ConnectionType.UPSTREAM_SYNC_BARRIER.ofType()){
			// one barrier for all connections within the same barrier
			LOG.info("Creating NETWORK inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.UPSTREAM_SYNC_BARRIER.withName());
			InputAdapter ia = new NetworkBarrier(wc, streamId, expectedSchema, upc);
			ias.add(ia);
		}
		else if(cType == ConnectionType.BATCH.ofType()){
			
		}
		else if(cType == ConnectionType.ORDERED.ofType()){
			
		}
		else if(cType == ConnectionType.WINDOW.ofType()){
			
		}
		return ias;
	}
	
	public static List<InputAdapter> buildInputAdapterOfTypeFileForOps(WorkerConfig wc, int streamId, List<UpstreamConnection> upc){
		List<InputAdapter> ias = new ArrayList<>();
		short cType = upc.get(0).getConnectionType().ofType();
		Schema expectedSchema = upc.get(0).getExpectedSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			LOG.info("Creating FILE inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.ONE_AT_A_TIME.withName());
			for(UpstreamConnection uc : upc){
				int opId = uc.getUpstreamOperator().getOperatorId();
				InputAdapter ia = new FileDataStream(wc, opId, streamId, expectedSchema);
				ias.add(ia);
			}
		}
		else if(cType == ConnectionType.BATCH.ofType()){
			
		}
		
		return ias;
	}
	
	public static List<InputAdapter> buildInputAdapterOfTypeKafkaForOps(WorkerConfig wc, int streamId, List<UpstreamConnection> upc){
		List<InputAdapter> ias = new ArrayList<>();
		short cType = upc.get(0).getConnectionType().ofType();
		Schema expectedSchema = upc.get(0).getExpectedSchema();
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			LOG.info("Creating KAFKA inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.ONE_AT_A_TIME.withName());
			for(UpstreamConnection uc : upc){
				int opId = uc.getUpstreamOperator().getOperatorId();
				// FIXME: get worker configs from WorkerConfig and KAFKA-specific configs from (KafkaConfig)uc.getDataOrigin().getConfig()
				InputAdapter ia = new KafkaDataStream(opId, streamId, expectedSchema);
				ias.add(ia);
			}
		}
		return ias;
	}
	
	public static List<InputAdapter> buildInputAdapterOfTypeHdfsForOps(WorkerConfig wc, Integer streamId, List<UpstreamConnection> upCon) {
		List<InputAdapter> ias = new ArrayList<>();
		short cType = upCon.get(0).getConnectionType().ofType();
		Schema expectedSchema = upCon.get(0).getExpectedSchema();
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			LOG.info("Creating HDFS inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.ONE_AT_A_TIME.withName());
			for(UpstreamConnection uc : upCon){
				int opId = uc.getUpstreamOperator().getOperatorId();
				int queuelength = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
				int headroom = wc.getInt(WorkerConfig.BATCH_SIZE)*2;
				InputAdapter ia = new HdfsInputAdapter(opId, streamId, expectedSchema,queuelength,headroom);
				ias.add(ia);
			}
		}
		return ias;
	}
}
