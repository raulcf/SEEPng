import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seepcontrib.kafka.KafkaSystemConsumer;


public class Sink implements SeepTask {
	
	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
	private KafkaSystemConsumer consumer;
	
	@Override
	public void setUp() {
		// TO DO: need to start kafka server
		String zookeeperServer = "localhost:2181";
		consumer = new KafkaSystemConsumer(zookeeperServer, "0", this, schema);
		consumer.subscribe("simple-kafka-stateless-pipeline-sink");
	}

	@Override
	public void processData(ITuple data, API api) {
		int userId = data.getInt("userId");
		long ts = data.getLong("ts");
		String text = data.getString("text");
		
		System.out.println("UID: "+userId+" ts: "+ts+" text: "+text);
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		consumer.stop();
	}

}
