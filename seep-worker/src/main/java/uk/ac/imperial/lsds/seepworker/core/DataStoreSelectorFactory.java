package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
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
			WorkerConfig wc, PhysicalOperator o, InetAddress myIp, int dataPort) {
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
			WorkerConfig wc, PhysicalOperator o, InetAddress myIp, int dataPort){
		NetworkSelector ns = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)){
			LOG.info("Configuring networkSelector for input");
			ns = new NetworkSelector(wc, o.getOperatorId(), coreInput.getInputAdapterProvider());
			ns.configureAccept(myIp, dataPort);
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)){
			LOG.info("Configuring networkSelector for output");
			if(ns == null) ns = new NetworkSelector(wc, o.getOperatorId(), coreInput.getInputAdapterProvider());
			Set<OutputBuffer> obufs = coreOutput.getOutputBuffers();
			ns.configureConnect(obufs);
		}
		return ns;
	}

	public static FileSelector maybeConfigureFileSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, PhysicalOperator o, InetAddress myIp, int dataPort){
		FileSelector fs = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.FILE)){
			fs = new FileSelector(wc);
			Map<Integer, DataStore> fileOrigins = new HashMap<>();
			for(UpstreamConnection uc : o.upstreamConnections()){
				int opId = uc.getUpstreamOperator().getOperatorId();
				if(uc.getDataOriginType() == DataStoreType.FILE) {
					fileOrigins.put(opId, uc.getDataOrigin());
				}
			}
			fs.configureAccept(fileOrigins, coreInput.getInputAdapterProvider());
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.FILE)){
			throw new NotImplementedException("not implemented yet...");
		}
		return fs;
	}

	public static KafkaSelector maybeConfigureKafkaSelector(CoreInput coreInput, CoreOutput coreOutput, 
			WorkerConfig wc, PhysicalOperator o, InetAddress myIp, int dataPort){
		KafkaSelector ks = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.KAFKA)){
			KafkaConfig kc = new KafkaConfig( o.upstreamConnections().get(0).getDataOrigin().getConfig() );
			LOG.info("Configuring kafkaSelector for input");
			ks = new KafkaSelector(kc.getString(KafkaConfig.BASE_TOPIC), kc.getString(KafkaConfig.ZOOKEEPER_CONNECT),
					kc.getString(KafkaConfig.CONSUMER_GROUP_ID), coreInput.getInputAdapterProvider());			
		}
		return ks;
	}
	
}
