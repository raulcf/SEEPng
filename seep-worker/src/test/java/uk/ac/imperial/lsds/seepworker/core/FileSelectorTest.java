package uk.ac.imperial.lsds.seepworker.core;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.tools.GenerateBinaryFile;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.core.input.FileDataStream;

public class FileSelectorTest {

//	@Test
//	public void test() {
//		// Create temp file with a given schema
//		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
//		String path = new String("/tmp/test.data");
//		int targetSize = 1000000;
//		GenerateBinaryFile.createFile(s, path, targetSize);
//		
//		int opId = 99;
//		FileSelector fs = new FileSelector(null);
//		InputAdapter ia = FileDataStream.getFileDataStream_test(0, opId, s, 100, 1000);
//		Map<Integer, InputAdapter> dataAdapters = new HashMap<>();
//		dataAdapters.put(0, ia);
//		String absPath = Utils.absolutePath(path);
//		URI uri = null;
//		try {
//			uri = new URI(Utils.FILE_URI_SCHEME + absPath);
//		} 
//		catch (URISyntaxException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		Path resource = Paths.get(uri);
//		
//		fs.addNewAccept(resource, 0, dataAdapters);
//		
//		fs.startFileSelector();
//		
//		while(true){
//			ITuple tuple = ia.pullDataItem(100);
//			if(tuple == null) continue;
//			int p1 = tuple.getInt("param1");
//			int p2 = tuple.getInt("param2");
//			System.out.println("P1: "+p1+" P2: "+p2);
//		}
//		
//	}

}
