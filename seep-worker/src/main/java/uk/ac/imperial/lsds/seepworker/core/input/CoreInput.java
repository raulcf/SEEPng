package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataOrigin;
import uk.ac.imperial.lsds.seep.api.DataOriginType;

public class CoreInput {
	
	final private static Logger LOG = LoggerFactory.getLogger(CoreInput.class);
	
	private List<InputAdapter> inputAdapters;
	private Map<Integer, InputAdapter> iapMap;
	
	public CoreInput(List<InputAdapter> inputAdapters) {
		this.inputAdapters = inputAdapters;
		iapMap = new HashMap<>();
		for(InputAdapter ia : inputAdapters) {
			for(Integer opId : ia.getRepresentedOpId()) {
				LOG.debug("Configure IA for opId: {}", opId);
				iapMap.put(opId, ia);
			}
		}
		LOG.info("Configured CoreInput with {} inputAdapters", inputAdapters.size());
	}
	
	public List<InputAdapter> getInputAdapters(){
		return inputAdapters;
	}
	
	public boolean requiresConfiguringNetworkWorker(){
		for(InputAdapter ia : inputAdapters){
			if(ia.requiresNetwork())
				return true;
		}
		return false;
	}
	
	public boolean requiresConfiguringFileWorker(){
		for(InputAdapter ia : inputAdapters){
			if(ia.requiresFile())
				return true;
		}
		return false;
	}
	
	public Map<Integer, InputAdapter> getInputAdapterProvider(){
		return iapMap;
	}
	
	public List<DataOrigin> getDataOriginOfType(DataOriginType type){
		List<DataOrigin> orgs = new ArrayList<>();
		
		return orgs;
	}
	
}
