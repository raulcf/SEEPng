package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class CoreInputFactory {

	final private static Logger LOG = LoggerFactory.getLogger(CoreInputFactory.class);

	public static CoreInput buildCoreInputForStage(WorkerConfig wc, Map<Integer, Set<DataReference>> input) {
		return new CoreInput(wc, input);
	}

	public static CoreInput buildCoreInputFor(WorkerConfig wc, Map<Integer, Set<DataReference>> input, Map<Integer, ConnectionType> connTypeInformation) {
		LOG.info("Building Core Input...");
		List<InputAdapter> inputAdapters = new LinkedList<>();
		for(Entry<Integer, Set<DataReference>> entry : input.entrySet()) {
			int streamId = entry.getKey();
			Set<DataReference> drefs = entry.getValue();
			List<InputAdapter> ias = InputAdapterFactory.buildInputAdapterForStreamId(wc, streamId, drefs, connTypeInformation.get(streamId));
			if(ias != null){
				inputAdapters.addAll(ias);
			}
		}
		CoreInput ci = new CoreInput(inputAdapters);
		LOG.info("Building Core Input...OK");
		return ci;
	}
	
}
