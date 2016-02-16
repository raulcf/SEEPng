package uk.ac.imperial.lsds.seepmaster.query;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManagerFactory;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.PhysicalNode;

public class QueryManagerTest {

	@Test
	public void testBase(){
		BaseTest fb = new BaseTest();
		SeepLogicalQuery lsq = fb.compose();
		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(0);
		Map<Integer, ControlEndPoint> mapOperatorToEndPoint = null;
		Comm cu = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		
		// Artificially populate infrastructure
		ExecutionUnit eu = null, eu1 = null, eu2 = null, eu3 = null, eu4 = null;
		try {
			eu = new PhysicalNode(InetAddress.getByName("10.0.0.1"), 3500, 5000);
			eu1 = new PhysicalNode(InetAddress.getByName("10.0.0.2"), 3501, 5001);
			eu2 = new PhysicalNode(InetAddress.getByName("10.0.0.3"), 3502, 5002);
			eu3 = new PhysicalNode(InetAddress.getByName("10.0.0.4"), 3503, 5003);
			eu4 = new PhysicalNode(InetAddress.getByName("10.0.0.5"), 3504, 5004);
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		inf.addExecutionUnit(eu);
		inf.addExecutionUnit(eu1);
		inf.addExecutionUnit(eu2);
		inf.addExecutionUnit(eu3);
//		inf.addExecutionUnit(eu4);
		
		// Build query manager
		MaterializedQueryManager qm = MaterializedQueryManager.buildTestMaterializedQueryManager(lsq, inf, mapOperatorToEndPoint, cu);
		
		// Use helper method to create physical query
		Map<Integer, ControlEndPoint> m = qm.createMappingOfOperatorWithEndPoint(lsq);
		for(Entry<Integer, ControlEndPoint> entry : m.entrySet()) {
			System.out.println("OPID: "+entry.getKey()+ " EP: "+entry.getValue());
		}
		assert(true);
	}

}
