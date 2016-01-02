package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.EventBasedOBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;
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
			ns = new NetworkSelector(wc, o.getOperatorId());
			ns.configureServerToListen(myIp, dataPort);
			ns.configureExpectedIncomingConnection(coreInput.getIBufferProvider());
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)) {
			LOG.info("Configuring networkSelector for output");
			Set<OBuffer> obufsToStream = coreOutput.getOBufferToDataStoreOfType(DataStoreType.NETWORK);
			// If ns is null create it first
			if(ns == null){
				ns = new NetworkSelector(wc, o.getOperatorId());
			}
			// TODO: maybe we can iterate directly over eventbasedOBuffer ?
			for(OBuffer ob : obufsToStream) {
				if (ob instanceof EventBasedOBuffer) {
					((EventBasedOBuffer)ob).setEventAPI(ns);
				}
			}
		}
		return ns;
	}
	
	public static FileSelector maybeConfigureFileSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, LogicalOperator o, InetAddress myIp, int dataPort) {
		FileSelector fs = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.FILE)) {
			fs = new FileSelector(wc);
			// DataReferenceId - DataStore
			Map<Integer, DataStore> fileOrigins = new HashMap<>();
			Map<Integer, Set<DataReference>> i_dRefs = coreInput.getDataReferences();
			
			for(Set<DataReference> dRefs : i_dRefs.values()) {
				for(DataReference dR : dRefs) {
					DataStore dataStore = dR.getDataStore();
					if(dataStore.type() == DataStoreType.FILE) {
						int dRefId = dR.getId();
						fileOrigins.put(dRefId, dataStore);
					}
				}
			}
			
			
//			for(DataReference dR : dRefs) {
//				DataStore dataStore = dR.getDataStore();
//				if(dataStore.type() == DataStoreType.FILE) {
//					int dRefId = dR.getId();
//					fileOrigins.put(dRefId, dataStore);
//				}
//			}
			
//			for(UpstreamConnection uc : o.upstreamConnections()) {
//				if(uc.getDataStoreType() == DataStoreType.FILE) {
//					int sId = uc.getStreamId();
//					fileOrigins.put(sId, uc.getDataStore());
//				}
//			}
			
			
			fs.configureAccept(fileOrigins, coreInput.getIBufferProvider());
		}
		// Output of type File is taken care of
//		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.FILE)) {
//			Set<OBuffer> obufsToStream = coreOutput.getOBufferToDataStoreOfType(DataStoreType.FILE);
//			// If fs is null create it first
//			if(fs == null) {
//				fs = new FileSelector(wc);
//			}
//			Map<Integer, DataStore> fileDest = coreOutput.getMapStreamIdToDataStore();
//			fs.configureDownstreamFiles(fileDest, obufsToStream);
//			// TODO: maybe we can iterate directly over eventbasedOBuffer ?
//			for(OBuffer ob : obufsToStream) {
//				if (ob instanceof EventBasedOBuffer) {
//					((EventBasedOBuffer)ob).setEventAPI(fs);
//				}
//			}
//		}
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
