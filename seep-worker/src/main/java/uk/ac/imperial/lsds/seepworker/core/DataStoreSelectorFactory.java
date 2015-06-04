package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seepcontrib.kafka.comm.KafkaSelector;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

public class DataStoreSelectorFactory {

	final private static Logger LOG = LoggerFactory.getLogger(DataStoreSelectorFactory.class.getName());
	
	public static List<DataStoreSelector> buildDataStoreSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, LogicalOperator o, InetAddress myIp, int dataPort) {
		List<DataStoreSelector> selectors = new ArrayList<>();
		
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.NETWORK) ||
		   coreOutput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)){
			DataStoreSelector sel = DataStoreSelectorFactory.maybeConfigureNetworkSelector(coreInput, coreOutput, 
					wc, o, myIp, dataPort);
			selectors.add(sel);
		}
		
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.FILE) ||
		   coreOutput.requiresConfigureSelectorOfType(DataStoreType.FILE)) {
			DataStoreSelector sel = DataStoreSelectorFactory.maybeConfigureFileSelector(coreInput, coreOutput, 
					wc, o, myIp, dataPort);
			selectors.add(sel);
		}
		
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.KAFKA)) {
			DataStoreSelector sel = DataStoreSelectorFactory.maybeConfigureKafkaSelector(coreInput, coreOutput, 
					wc, o, myIp, dataPort);
			selectors.add(sel);
		}
		
		return selectors;
	}
	
	public static NetworkSelector maybeConfigureNetworkSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, LogicalOperator o, InetAddress myIp, int dataPort){
		NetworkSelector ns = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)) {
			LOG.info("Configuring networkSelector for input");
			ns = new NetworkSelector(wc, o.getOperatorId(), coreInput.getIBufferProvider());
			ns.configureServerToListen(myIp, dataPort);
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)) {
			LOG.info("Configuring networkSelector for output");
			if(ns == null){
				Set<OBuffer> obufsToStream = filterOBufferToStream(coreOutput.getBuffers());
				if(obufsToStream.size() > 0) {
					ns = new NetworkSelector(wc, o.getOperatorId(), coreInput.getIBufferProvider());
					for(OBuffer ob : obufsToStream) {
						ob.setEventAPI(ns);
					}
				}
			}
		}
		return ns;
	}

	private static Set<OBuffer> filterOBufferToStream(Map<Integer, OBuffer> buffers) {
		// Select only those that are meant to be streamed
		Set<OBuffer> filtered = new HashSet<>();
		for(OBuffer oBuffer : buffers.values()) {
			if(oBuffer.getDataReference().getServeMode().equals(ServeMode.STREAM)) {
				filtered.add(oBuffer);
			}
		}
		return filtered;
	}

	public static FileSelector maybeConfigureFileSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, LogicalOperator o, InetAddress myIp, int dataPort){
		FileSelector fs = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.FILE)){
			fs = new FileSelector(wc);
			Map<Integer, DataStore> fileOrigins = new HashMap<>();
			for(UpstreamConnection uc : o.upstreamConnections()){
				int opId = uc.getUpstreamOperator().getOperatorId();
				if(uc.getDataStoreType() == DataStoreType.FILE) {
					fileOrigins.put(opId, uc.getDataStore());
				}
			}
			fs.configureAccept(fileOrigins, coreInput.getIBufferProvider());
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.FILE)){
			throw new NotImplementedException("not implemented yet...");
		}
		return fs;
	}

	public static KafkaSelector maybeConfigureKafkaSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, LogicalOperator o, InetAddress myIp, int dataPort){
		KafkaSelector ks = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.KAFKA)){
			KafkaConfig kc = new KafkaConfig( o.upstreamConnections().get(0).getDataStore().getConfig() );
			LOG.info("Configuring kafkaSelector for input");
			ks = new KafkaSelector(kc.getString(KafkaConfig.BASE_TOPIC), kc.getString(KafkaConfig.ZOOKEEPER_CONNECT),
					kc.getString(KafkaConfig.CONSUMER_GROUP_ID), coreInput.getIBufferProvider());
		}
		return ks;
	}
	
}
