package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.OBuffer;

public class CoreOutput {
	
	final private static Logger LOG = LoggerFactory.getLogger(CoreOutput.class);
	
	private Map<Integer, Set<DataReference>> output;
	private Map<Integer, OBuffer> oBuffers;
	private Map<Integer, List<OBuffer>> streamId_To_OBuffers;
		
	public CoreOutput(Map<Integer, Set<DataReference>> output, Map<Integer, List<OBuffer>> streamId_To_OBuffers, Map<Integer, OBuffer> oBuffers){
		this.output = output;
		this.oBuffers = oBuffers;
		this.streamId_To_OBuffers = streamId_To_OBuffers;
		LOG.info("Configured CoreOutput with {} outputAdapters", streamId_To_OBuffers.size());
	}
	
	public Map<Integer, List<OBuffer>> getOutputAdapters() {
		return streamId_To_OBuffers;
	}
	
	public Map<Integer, List<OBuffer>> getStreamIdToBuffers() {
		return streamId_To_OBuffers;
	}

	public Map<Integer, OBuffer> getBuffers() {
		return oBuffers;
	}

	public boolean requiresConfigureSelectorOfType(DataStoreType type) {
		for(Set<DataReference> dres : output.values()) {
			for(DataReference dr : dres) {
				if(dr.getDataStore().type().equals(type)) {
					return true;
				}
			}
		}
		return false;
	}
	
//	public Set<OutputBuffer> getOutputBuffers(){
//		Set<OutputBuffer> cons = new HashSet<>();
//		for(OutputAdapter oa : outputAdapters){
//			cons.addAll(oa.getOutputBuffers().values());
//		}
//		return cons;
//	}
//	
//	public boolean requiresConfigureSelectorOfType(DataStoreType type){
//		for(OutputAdapter oa : outputAdapters){
//			if(oa.getDataOriginType().equals(type)){
//				return true;
//			}
//		}
//		return false;
//	}
//	
//	public void setEventAPI(EventAPI eAPI){
//		for(OutputAdapter oa : outputAdapters){
//			oa.setEventAPI(eAPI);
//		}
//	}
	
}
