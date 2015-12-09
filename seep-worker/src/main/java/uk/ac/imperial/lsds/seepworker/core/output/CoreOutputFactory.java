package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;

public class CoreOutputFactory {

	final private static Logger LOG = LoggerFactory.getLogger(CoreOutputFactory.class);
	
	public static CoreOutput buildCoreOutputFor(WorkerConfig wc, DataReferenceManager drm, Map<Integer, Set<DataReference>> output) {
		LOG.info("Building coreOutput...");
		Map<Integer, OBuffer> oBuffers = new HashMap<>();
		Map<Integer, List<OBuffer>> streamId_To_OBuffers = new HashMap<>();
		for(Entry<Integer, Set<DataReference>> entry : output.entrySet()) {
			int streamId = entry.getKey();
			List<OBuffer> buffers = new ArrayList<>();
			for(DataReference dr : entry.getValue()) {
				OBuffer ob = null;
				// We indicate DRM to manage a new DataReference and get a Dataset that will host the data in return
				if(dr.getServeMode().equals(ServeMode.STORE)) {
					ob = drm.manageNewDataReference(dr);
				}
				else if (dr.getServeMode().equals(ServeMode.SINK)) {
					ob = new OutputBuffer(dr, wc.getInt(WorkerConfig.BATCH_SIZE));
				}
				// We simply create an outputBuffer that will host the data that will ship to an external system
				else if(dr.getServeMode().equals(ServeMode.STREAM)) {
					ob = new OutputBuffer(dr, wc.getInt(WorkerConfig.BATCH_SIZE));
				}
				oBuffers.put(dr.getId(), ob); // dr.id -> obuffer
				buffers.add(ob);
			}
			streamId_To_OBuffers.put(streamId, buffers);
		}
		CoreOutput cOutput = new CoreOutput(output, streamId_To_OBuffers, oBuffers);
		LOG.info("Building coreOutput...OK");
		return cOutput;
	}
	
}
