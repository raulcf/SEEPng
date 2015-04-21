import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.MRBuilder;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.MapperTask;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.ReducerTask;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;

public class Base implements QueryComposer {

	@Override
	public LogicalSeepQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.STRING, "key").newField(Type.INT, "value").build();
		
		Properties p1 = new Properties();
		p1.setProperty(HdfsConfig.HDFS_SERVER, "localhost:9000");
		//Specify the address of current file abs location on HDFS.
		p1.setProperty(HdfsConfig.HDFS_PATH,"/test.txt");
		p1.setProperty(HdfsConfig.HDFS_TEXT, "TEXT");
		
		MapperTask mt = new MapperTask();
		ReducerTask rt = new ReducerTask();
		
		Properties p2 = new Properties();
		p2.setProperty(HdfsConfig.HDFS_SERVER, "localhost:9000");
		p2.setProperty(HdfsConfig.HDFS_PATH, "/result.txt");
		p2.setProperty(HdfsConfig.HDFS_TEXT, "NONTEXT");
		
		MRBuilder.configure(mt,rt,schema,p1,p2);
		
		return queryAPI.build();
	}

}