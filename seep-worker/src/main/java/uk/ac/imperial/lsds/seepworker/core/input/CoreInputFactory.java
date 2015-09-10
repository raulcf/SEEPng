package uk.ac.imperial.lsds.seepworker.core.input;

import java.awt.dnd.DragSourceMotionListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;

public class CoreInputFactory {

	final private static Logger LOG = LoggerFactory.getLogger(CoreInputFactory.class);

	public static CoreInput buildCoreInputFor(WorkerConfig wc, DataReferenceManager drm, Map<Integer, Set<DataReference>> input, Map<Integer, ConnectionType> connTypeInformation) {
		LOG.info("Building Core Input...");
		List<InputAdapter> inputAdapters = new LinkedList<>();
		Map<Integer, IBuffer> iBuffers = new HashMap<>();
		for(Entry<Integer, Set<DataReference>> entry : input.entrySet()) {
			int streamId = entry.getKey();
			Set<DataReference> drefs = entry.getValue();
			List<IBuffer> buffers = new ArrayList<>();
			for(DataReference dr : drefs) {
				IBuffer ib = null;
				// If DR is managed internally and locally
				if(drm.doesManageDataReference(dr.getId()) != null) {
					ib = drm.getInputBufferFor(dr);
				}
				// If not
				else {
					ib = InputBuffer.makeInputBufferFor(wc, dr);
				}
				iBuffers.put(dr.getId(), ib); // dataref.id -> inputbuffer
				buffers.add(ib);
			}
			ConnectionType ct = connTypeInformation.get(streamId);
			List<InputAdapter> ias = InputAdapterFactory.buildInputAdapterForStreamId(wc, streamId, buffers, drefs, ct);
			inputAdapters.addAll(ias);
		}
		CoreInput ci = new CoreInput(wc, input, iBuffers, inputAdapters);
		LOG.info("Building Core Input...OK");
		return ci;
	}
	
}
