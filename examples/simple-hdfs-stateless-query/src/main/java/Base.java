import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.FileConfig;
import uk.ac.imperial.lsds.seep.api.FileSource;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;
import uk.ac.imperial.lsds.seep.api.HdfsFileSource;
import uk.ac.imperial.lsds.seep.api.HdfsSink;

public class Base implements QueryComposer {

	@Override
	public LogicalSeepQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
		
		Properties p = new Properties();
		p.setProperty(HdfsConfig.HDFS_SERVER, "localhost:9000");
		//Specify the address of current file abs location on HDFS.
		p.setProperty(HdfsConfig.HDFS_PATH,"/test.txt");
		p.setProperty(HdfsConfig.HDFS_TEXT,"NONTEXT");
		HdfsFileSource hfileSource = HdfsFileSource.newSource(0, p);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		Properties p2 = new Properties();
		p2.setProperty(HdfsConfig.HDFS_SERVER, "localhost:9000");
		p2.setProperty(HdfsConfig.HDFS_PATH, "/result.txt");
		p2.setProperty(HdfsConfig.HDFS_TEXT,"NONTEXT");
		HdfsSink hsink = HdfsSink.newSink(2,p2);
		
		hfileSource.connectTo(processor, 0, schema);
		//Further, pass new DataStoreType to this function to perform HDFS interaction.
		processor.connectTo(hsink, 0, schema);
		
		return queryAPI.build();
	}

}