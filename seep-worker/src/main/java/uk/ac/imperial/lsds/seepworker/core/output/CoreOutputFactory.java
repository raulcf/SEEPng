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
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;

public class CoreOutputFactory {

	final private static Logger LOG = LoggerFactory.getLogger(CoreOutputFactory.class);
	
	public static CoreOutput buildCoreOutputFor(WorkerConfig wc, 
			DataReferenceManager drm, 
			Map<Integer, Set<DataReference>> output) {
		LOG.info("Building coreOutput...");
		Map<Integer, OBuffer> oBuffers = new HashMap<>();
		Map<Integer, List<OBuffer>> streamId_To_OBuffers = new HashMap<>();
		// Iterate per streamId
		for(Entry<Integer, Set<DataReference>> entry : output.entrySet()) {
			int streamId = entry.getKey();
			List<OBuffer> buffers = new ArrayList<>();
			for(DataReference dr : entry.getValue()) {
				OBuffer ob = null;
				// If STORE, then DRM is responsible for the new DataReference
				if(dr.getServeMode().equals(ServeMode.STORE)) {
					ob = drm.manageNewDataReference(dr);
				}
				// If SINK, then it will depend on the target DataStoreType
				else if (dr.getServeMode().equals(ServeMode.SINK)) {
					// TODO: refactor this into a another private method below
					DataStoreType type = dr.getDataStore().type();
					if(type.equals(DataStoreType.FILE)) {
						FileConfig config = new FileConfig(dr.getDataStore().getConfig());
						if (config.getBoolean(FileConfig.TEXT_SOURCE)) {
							ob = new TextFileOutputBuffer(dr, wc.getInt(WorkerConfig.BATCH_SIZE));
						} else {
							ob = new FileOutputBuffer(dr, wc.getInt(WorkerConfig.BATCH_SIZE));
						}
					}
					else {
						ob = new NullOutputBuffer();
					}
				}
				// If STREAM, data is kept in an OutputBuffer until the network services pulls it
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
