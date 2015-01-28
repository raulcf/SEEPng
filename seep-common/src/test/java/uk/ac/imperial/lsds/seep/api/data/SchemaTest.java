package uk.ac.imperial.lsds.seep.api.data;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class SchemaTest {

	@Test
	public void sizeOfTest(){
		
		String a = "hola";
		int size = Type.STRING.sizeOf(a);
		assert(size == 8);
		
	}
	
	@Test
	public void testCreateSchema() {
		SchemaBuilder sb = SchemaBuilder.getInstance();
		Schema s = sb.newField(Type.INT, "userId").newField(Type.LONG, "timestamp").newField(Type.STRING, "text").build();
		
		s.toString();
		
		assert(s.getField("userId").equals(Type.INT));
		assert(s.getField("timestamp").equals(Type.LONG));
		assert(s.getField("text").equals(Type.STRING));
	}
	
	@Test
	public void testWriteAndReadTypes(){
		
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
		// Byte 
		byte byteWrite = 1;
		Type.BYTE.write(buffer, byteWrite);
		buffer.position(0);
		byte byteRead = (byte) Type.BYTE.read(buffer);
		
		assertEquals(byteWrite, byteRead);
		assertEquals(Type.BYTE.sizeOf(null), 1);
		
		buffer.clear();
		
		// Short
		short shortWrite = 5;
		Type.SHORT.write(buffer, shortWrite);
		buffer.position(0);
		short shortRead = (short) Type.SHORT.read(buffer);
		
		assertEquals(shortWrite, shortRead);
		assertEquals(Type.SHORT.sizeOf(null), 2);
		
		buffer.clear();
		
		// Int
		int intWrite = 5;
		Type.INT.write(buffer, intWrite);
		buffer.position(0);
		int intRead = (int) Type.INT.read(buffer);
		
		assertEquals(intWrite, intRead);
		assertEquals(Type.INT.sizeOf(null), 4);
		
		buffer.clear();
		
		// Long
		long longWrite = 5;
		Type.LONG.write(buffer, longWrite);
		buffer.position(0);
		long longRead = (long) Type.LONG.read(buffer);
		
		assertEquals(longWrite, longRead);
		assertEquals(Type.LONG.sizeOf(null), 8);
		
		buffer.clear();
		
		// String short
		String stringWrite = "hola";
		Type.STRING.write(buffer, stringWrite);
		buffer.position(0);
		String stringRead = (String) Type.STRING.read(buffer);
		
		assert(stringWrite.equals(stringRead));
		assertEquals(Type.STRING.sizeOf(stringWrite), stringWrite.length()+4);
		
		buffer.clear();
		
		// String longer
		String stringWrite2 = "asd;lf kl;qwe l;kajsdfw3efnmoj;al gwegq;glkqj fqfh;lkasdjf fqwphe;klh";
		Type.STRING.write(buffer, stringWrite2);
		buffer.position(0);
		String stringRead2 = (String) Type.STRING.read(buffer);
		
		assert(stringWrite2.equals(stringRead2));
		assertEquals(Type.STRING.sizeOf(stringWrite2), stringWrite2.length()+4);
		
		buffer.clear();
		
		// Array of Type
		Integer[] w = new Integer[]{34, 56, 45, 01};
		Array type = new Array(Type.INT);
		type.write(buffer, w);
		buffer.flip();
		Object[] readW = type.read(buffer);
		for(int i = 0; i<w.length; i++){
			System.out.println("Original: "+w[i]+" read: "+readW[i]);
			assertEquals(w[i], readW[i]);
		}
		
		// bytes[]
//		byte[] write = new byte[5];
//		ByteBuffer writeWrap = ByteBuffer.wrap(write);
//		Type.BYTES.write(buffer, writeWrap);
//		buffer.position(0);
//		ByteBuffer readWrap = (ByteBuffer) Type.BYTES.read(buffer);
//		byte[] read = readWrap.compact().array();
//		
//		assertEquals(write.length, read.length);
//		assertEquals(Type.BYTES.sizeOf(write), Type.BYTES.sizeOf(read));
//		
//		buffer.clear();
		
		
	}
	
	@Test
	public void simpleWriteAndReadAndPrintTest() {
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		byte[] sData = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{666, 333333L});
		
		ITuple i = new ITuple(s);
		i.setData(sData);
		String tuple = i.toString();
//		System.out.println(tuple);
		
		assertTrue(true);
	}
	
	@Test
	public void writeAndReadFixedSizeSchemaTest(){
		
		// Fixed size schema
		int u = 15;
		long t = System.currentTimeMillis();
		
		Schema outputSchema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		OTuple output = new OTuple(outputSchema);
		
		byte[] serializedData = OTuple.create(outputSchema, new String[]{"userId", "ts"}, new Object[]{u, t});
		
		ITuple input = new ITuple(outputSchema); // share output and input schema in the simplest case
		input.setData(serializedData);
		
		int userId = input.getInt("userId");
		long ts = input.getLong("ts");
		
		assertEquals(u, userId);
		assertEquals(t, ts);
	}
	
	@Test
	public void writeAndReadVariableSizeSchemaTest(){
		Schema vs = SchemaBuilder.getInstance().newField(Type.STRING, "item").newField(Type.INT, "price").build();
		// Variable size schema
		String item = "pc";
		int price = 250;
		
		byte[] serializedData = OTuple.create(vs, new String[]{"item", "price"}, new Object[]{item, price});
		
		ITuple i = new ITuple(vs);
		i.setData(serializedData);
		
		String _item = i.getString("item");
		int _price = i.getInt("price");
		System.out.println("Item: "+_item+" costs: "+_price);
		
		assert(_item.equals(item));
		assert(_price == price);
	}

}
