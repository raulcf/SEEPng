package uk.ac.imperial.lsds.seep.comm.serialization.kryoserializers;

import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MaterializeTaskCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;

public class MaterializeTaskCommandSerializerTest {

	public MasterWorkerCommand get() {
		Map<Integer, SeepEndPoint> mapping = new HashMap<>();
		InetAddress ip = null;
		try {
			ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mapping.put(1, new DataEndPoint(2000, ip.getHostAddress(), 3000));
		Set<DataReference> r = new HashSet<>();
//		r.add(DataReference.makeExternalDataReference(null, new EndPoint(1000, ip, 1000)));
		r.add(DataReference.makeExternalDataReference(null));
		Map<Integer, Set<DataReference>> refs = new HashMap<>();
		refs.put(3, r);
		Map<Integer, Map<Integer, Set<DataReference>>> inputs = new HashMap<>();
		inputs.put(666, refs);
		Map<Integer, Map<Integer, Set<DataReference>>> outputs = new HashMap<>();
		outputs.put(777, refs);
		
		return (MasterWorkerCommand) ProtocolCommandFactory.buildMaterializeTaskCommand(mapping, inputs, outputs);
	}
	
	@Test
	public void test() {
		
//		MasterWorkerCommand mtc = get();
//		
//		// Serialise object
//		Kryo k = KryoFactory.buildKryoForMasterWorkerProtocol();
//		ByteBufferOutputStream o = new ByteBufferOutputStream(20000);
//		Output output = new Output(o);
//		k.writeObject(output, mtc, new MaterializeTaskCommandSerializer());
//		output.flush();
//		
//		//copy output to input
//		byte[] data = o.getByteBuffer().array();
//		
//		ByteBufferInputStream i = new ByteBufferInputStream();
//		i.setByteBuffer(ByteBuffer.wrap(data));
//		
//		// Deserialise object
//		
//		Input input = new Input(i);
//		MasterWorkerCommand d = k.readObject(input, mtc.getClass(), new MaterializeTaskCommandSerializer());
//		MaterializeTaskCommand de = d.getMaterializeTaskCommand();
//		
//		assertTrue(de.getMapping() != null);
////		assertTrue(de.getInputs() != null);
//		assertTrue(de.getOutputs() != null);
		
	}
	
	@Test
	public void test2() {
		
//		MasterWorkerCommand mtc = get();
//		
//		// Serialise object
//		Kryo k = KryoFactory.buildKryoForMasterWorkerProtocol();
//		ByteBufferOutputStream o = new ByteBufferOutputStream(20000);
//		Output output = new Output(o);
//		k.writeObject(output, mtc, new com.esotericsoftware.kryo.serializers.JavaSerializer());
//		output.flush();
//		
//		//copy output to input
//		byte[] tx = o.getByteBuffer().array();
//		
//		ByteBufferInputStream i = new ByteBufferInputStream();
//		i.setByteBuffer(ByteBuffer.wrap(tx));
//		
//		// Deserialise object
//		Input input = new Input(i);
//		MasterWorkerCommand d = k.readObject(input, mtc.getClass(), new MaterializeTaskCommandSerializer());
//		MaterializeTaskCommand de = d.getMaterializeTaskCommand();
//		
//		assertTrue(de.getMapping() != null);
////		assertTrue(de.getInputs() != null);
//		assertTrue(de.getOutputs() != null);
	}
	
	@Test
	public void test3(){
		MasterWorkerCommand mtc = get();
		
		// Serialise object
		Kryo k = KryoFactory.buildKryoForMasterWorkerProtocol();
		ByteBufferOutputStream o = new ByteBufferOutputStream(20000);
		Output output = new Output(o);
		k.writeObject(output, mtc);
		output.flush();
		
		//copy output to input
		byte[] tx = o.getByteBuffer().array();
		
		ByteBufferInputStream i = new ByteBufferInputStream();
		i.setByteBuffer(ByteBuffer.wrap(tx));
		
		// Deserialise object
		Input input = new Input(i);
		MasterWorkerCommand d = k.readObject(input, mtc.getClass());
		MaterializeTaskCommand de = d.getMaterializeTaskCommand();
		
		assertTrue(de.getMapping() != null);
		assertTrue(de.getInputs() != null);
		assertTrue(de.getOutputs() != null);
		
		
	}

}
