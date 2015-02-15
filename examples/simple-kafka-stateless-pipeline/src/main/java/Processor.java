import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seepcontrib.kafka.KafkaSystemConsumer;
import uk.ac.imperial.lsds.seepcontrib.kafka.KafkaSystemProducer;

public class Processor implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
	private KafkaSystemConsumer consumer;
	private KafkaSystemProducer producer;
	
	@Override
	public void setUp() {
		String zookeeperServer = "localhost:2181";
		consumer = new KafkaSystemConsumer(zookeeperServer, "0", this, schema);
		consumer.subscribe("simple-kafka-stateless-pipeline-source");
		String kafkaServer = "localhost:9092";
		producer = new KafkaSystemProducer(kafkaServer, "0");
		producer.register("simple-kafka-stateless-pipeline-sink");
	}

	@Override
	public void processData(ITuple data, API api) {
		int userId = data.getInt("userId");
		long ts = data.getLong("ts");
		String text = data.getString("text");
		text = text + "_processed";
		
		userId = userId + userId;
		ts = ts - 1;
		
		byte[] processedData = OTuple.create(schema, new String[]{"userId", "ts", "text"},  new Object[]{userId, ts, text});
		producer.send("key", processedData);
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		consumer.stop();
		producer.stop();
	}
}