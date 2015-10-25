package uk.ac.imperial.lsds.seepworker.core.input;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;
import uk.ac.imperial.lsds.seepworker.core.Dataset;

public class DatasetInputAdapterTest {

	@Test
	public void test() {
		Properties p = new Properties();
		p.setProperty(WorkerConfig.LISTENING_IP, "");
		p.setProperty(WorkerConfig.MASTER_IP, "");
		p.setProperty(WorkerConfig.PROPERTIES_FILE, "");
		WorkerConfig wc = new WorkerConfig(p);
		
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(wc);
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "uid").newField(Type.LONG, "ts").build();
		DataStore dataStore = new DataStore(schema, DataStoreType.IN_MEMORY); // how to get this
		InetAddress whatever = null;
		try {
			whatever = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		EndPoint endPoint = new EndPoint(0, whatever, 0, 0); // me
		DataReference dr = DataReference.makeManagedDataReference(dataStore, endPoint, ServeMode.STORE);
		
		
		// to write to
		OBuffer ob = drm.manageNewDataReference(dr);
		
		int uid = 1;
		long ts = 1;
		byte[] ot1 = OTuple.create(schema, new String[]{"uid", "ts"}, new Object[]{uid,ts});
		uid = 2;
		ts = 2;
		byte[] ot2 = OTuple.create(schema, new String[]{"uid", "ts"}, new Object[]{uid,ts});
		uid = 3;
		ts = 3;
		byte[] ot3 = OTuple.create(schema, new String[]{"uid", "ts"}, new Object[]{uid,ts});
		ob.write(ot1);
		ob.write(ot2);
		ob.write(ot3);
		
		// to read from
		IBuffer ib = drm.getInputBufferFor(dr);
		
		DatasetInputAdapter dia = new DatasetInputAdapter(wc, 0, ((Dataset)ib));
		
		ITuple iTuple = dia.pullDataItem(100);
		int uid1 = iTuple.getInt("uid");
		long ts1 = iTuple.getLong("ts");
		System.out.println("1- UID: " + uid1 + ", 2- TS: " + ts1);
		
		iTuple = dia.pullDataItem(100);
		int uid2 = iTuple.getInt("uid");
		long ts2 = iTuple.getLong("ts");
		System.out.println("2- UID: " + uid2 + ", 2- TS: " + ts2);

		iTuple = dia.pullDataItem(100);
		int uid3 = iTuple.getInt("uid");
		long ts3 = iTuple.getLong("ts");
		System.out.println("3- UID: " + uid3 + ", 2- TS: " + ts3);
		
	}

}
