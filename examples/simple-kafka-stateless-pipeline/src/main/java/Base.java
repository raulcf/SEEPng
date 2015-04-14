import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;


public class Base implements QueryComposer {

	private final Properties p;
	
	public Base() {
		
		Properties p = new Properties();
		p.setProperty(KafkaConfig.KAFKA_SERVER, "localhost:9092");
		p.setProperty(KafkaConfig.ZOOKEEPER_CONNECT, "localhost:2181");
		p.setProperty(KafkaConfig.PRODUCER_CLIENT_ID, "seep");
		p.setProperty(KafkaConfig.CONSUMER_GROUP_ID, "seep");
		p.setProperty(KafkaConfig.BASE_TOPIC, "seep");
		
		// TODO: have a validator method in KafkaConfig here...
		
		this.p = p;
	}
	
	@Override
	public LogicalSeepQuery compose() {
		System.out.println("[Base] Start to build query");
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts")
												   .newField(Type.STRING, "text").build();
		
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), 0);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 2);
		
		src.connectTo(processor, 0, schema, new DataStore(DataStoreType.KAFKA, p));
		processor.connectTo(snk, 0, schema, new DataStore(DataStoreType.KAFKA, p));
		
		System.out.println("###### Build query finished");
		return queryAPI.build();
	}

}
