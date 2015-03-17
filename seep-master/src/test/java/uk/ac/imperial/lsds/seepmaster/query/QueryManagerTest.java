package uk.ac.imperial.lsds.seepmaster.query;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManagerFactory;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.PhysicalNode;

public class QueryManagerTest {

	@Test
	public void testBase(){
		BaseTest fb = new BaseTest();
		LogicalSeepQuery lsq = fb.compose();
		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(0);
		Map<Integer, EndPoint> mapOperatorToEndPoint = null;
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
		QueryManager qm = new QueryManager(lsq, inf, mapOperatorToEndPoint, cu);
		
		// Use helper method to create physical query
		PhysicalSeepQuery psq = qm.createOriginalPhysicalQueryFrom(lsq);
		System.out.println(psq.toString());
		assert(true);
	}
	
//	@Test
//	public void testFileBase(){
//		FileBase fb = new FileBase();
//		LogicalSeepQuery lsq = fb.compose();
//		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(0);
//		Map<Integer, EndPoint> mapOperatorToEndPoint = null;
//		Comm cu = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
//		
//		// Artificially populate infrastructure
//		ExecutionUnit eu = null, eu1 = null, eu2 = null, eu3 = null;
//		try {
//			eu = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3500, 5000);
//			eu1 = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3501, 5001);
//			eu2 = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3502, 5002);
//			eu3 = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3503, 5003);
//		}
//		catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//		
//		inf.addExecutionUnit(eu);
//		inf.addExecutionUnit(eu1);
//		inf.addExecutionUnit(eu2);
//		inf.addExecutionUnit(eu3);
//		
//		// Build query manager
//		QueryManager qm = new QueryManager(lsq, inf, mapOperatorToEndPoint, cu);
//		
//		// Use helper method to create physical query
//		PhysicalSeepQuery psq = qm.createOriginalPhysicalQueryFrom(lsq);
//		System.out.println(psq.toString());
//		assert(true);
//	}
	
//	@Test
//	public void testStatefulBase(){
//		StatefulBaseTest fb = new StatefulBaseTest();
//		LogicalSeepQuery lsq = fb.compose();
//		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(0);
//		Map<Integer, EndPoint> mapOperatorToEndPoint = null;
//		Comm cu = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
//		
//		// Artificially populate infrastructure
//		ExecutionUnit eu = null, eu1 = null, eu2 = null, eu3 = null;
//		try {
//			eu = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3500, 5000);
//			eu1 = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3501, 5001);
//			eu2 = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3502, 5002);
//			eu3 = new PhysicalNode(InetAddress.getByName("127.0.0.1"), 3503, 5003);
//		}
//		catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//		
//		inf.addExecutionUnit(eu);
//		inf.addExecutionUnit(eu1);
//		inf.addExecutionUnit(eu2);
//		inf.addExecutionUnit(eu3);
//		
//		// Build query manager
//		QueryManager qm = new QueryManager(lsq, inf, mapOperatorToEndPoint, cu);
//		
//		// Use helper method to create physical query
//		PhysicalSeepQuery psq = qm.createOriginalPhysicalQueryFrom(lsq);
//		System.out.println(psq.toString());
//		assert(true);
//	}

}
