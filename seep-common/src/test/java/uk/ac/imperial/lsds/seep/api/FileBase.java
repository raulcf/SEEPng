package uk.ac.imperial.lsds.seep.api;

import java.util.List;
import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.sinks.TagSink;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileSource;

public class FileBase implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		Schema s = SchemaBuilder.getInstance().newField(Type.LONG, "w1").newField(Type.LONG, "w2").build();
		
		// Create FileConfig to configure the file source
		Properties p = new Properties();
		p.setProperty(FileConfig.FILE_PATH, "/data/test.txt");
		p.setProperty(FileConfig.SERDE_TYPE, "0");
		
		FileSource source = FileSource.newSource(10, p);
		LogicalOperator trainer = queryAPI.newStatelessOperator(new Trainer(), 0);
		LogicalOperator parameterServer = queryAPI.newStatelessOperator(new ParameterServer(), 1);
		// Used to indicate a sink point
		TagSink sink = TagSink.newSink(2);
		
		source.connectTo(trainer, s, 0);
		trainer.connectTo(parameterServer, 0, new DataStore(s, DataStoreType.NETWORK), ConnectionType.UPSTREAM_SYNC_BARRIER);
		parameterServer.connectTo(trainer, 1, new DataStore(s, DataStoreType.NETWORK));
		// reusing properties to confgure the sink
		p.setProperty(FileConfig.FILE_PATH, "/data/output.txt");
		parameterServer.connectTo(sink, 10, new DataStore(s, DataStoreType.FILE, p));
		
		return queryAPI.build();
	}
	
	class Trainer implements SeepTask {
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(List<ITuple> dataBatch, API api) {		}
		@Override
		public void close() {		}
	}
	
	class ParameterServer implements SeepTask {
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(List<ITuple> dataBatch, API api) {		}
		@Override
		public void close() {		}
	}
	
	class Sink implements uk.ac.imperial.lsds.seep.api.operator.sinks.Sink {
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(List<ITuple> dataBatch, API api) {		}
		@Override
		public void close() {		}
	}

}
