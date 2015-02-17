package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.Map;

import uk.ac.imperial.lsds.seep.core.OutputBuffer;

public interface Router {

	public OutputBuffer route(Map<Integer, OutputBuffer> obufs);
	public OutputBuffer route(Map<Integer, OutputBuffer> obufs, int key);
	
}
