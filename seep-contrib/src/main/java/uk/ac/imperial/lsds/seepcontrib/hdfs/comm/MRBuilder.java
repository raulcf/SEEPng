package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.HdfsFileSource;
import uk.ac.imperial.lsds.seep.api.HdfsSink;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;

public class MRBuilder {

	public static void configure(MapperTask mt,ReducerTask rt,Schema schema,Properties src,Properties dest){
		SeepMapper spm = SeepMapper.newSeepMapper(mt,1,src,schema);
		SeepReducer spr = SeepReducer.newSeepReducer(rt, 2,schema);
		//SeepReducer spr2 = SeepReducer.newSeepReducer(rt, 3,schema);
		HdfsFileSource fsrc = HdfsFileSource.newSource(0, src);
		
		Properties fakedest = new Properties();
		fakedest.setProperty(HdfsConfig.HDFS_SERVER, "localhost:9000");
		fakedest.setProperty(HdfsConfig.HDFS_PATH,"/MRfakedest.txt");
		fakedest.setProperty(HdfsConfig.HDFS_TEXT, "NTEXT");
		HdfsSink fsink = HdfsSink.newSink(3, fakedest);
		
		fsrc.connectTo(spm, 0, schema);
		spm.connectTo(spr, 0, schema);//When defining the stream id between the mapper and the reducer, please start from 0.
		spr.connectTo(fsink, 1, schema);
		System.out.println("Configuring completed.");
		
	}
	
}
