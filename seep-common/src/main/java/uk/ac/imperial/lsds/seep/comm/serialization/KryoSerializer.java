package uk.ac.imperial.lsds.seep.comm.serialization;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer<T> implements Serializer<T> {

	private Kryo k;
	private static ByteBuffer bb;
	private static ByteBufferInputStream bbis;
	private static ByteBufferOutputStream bbos;
	
	private KryoSerializer(Kryo k){
		this.k = k;
		bb = ByteBuffer.allocate(1024);
		bbis = new ByteBufferInputStream(bb);
		bbos = new ByteBufferOutputStream(bb);
	}
	
	public static <T> KryoSerializer<T> getTypedSerializer(Object... classesToRegister){
		Kryo k = KryoFactory.buildKryoWithRegistration(classesToRegister);
		return new KryoSerializer<T>(k);
	}
	
	public static KryoSerializer<? extends Object> getSerializer(Object... classesToRegister){
		Kryo k = KryoFactory.buildKryoWithRegistration(classesToRegister);
		return new KryoSerializer(k);
	}

	@Override
	public byte[] serialize(T object) {
		bb.clear();
		Output output = new Output(bbos);
		k.writeObject(output, object);
		output.flush();
		byte[] data = output.getBuffer();
		return data;
	}

	@Override
	public T deserialize(byte[] data, Class<T> type) {
		bb.clear();
		Input input = new Input(bbis);
		T t = (T) k.readObject(input, type);
		return t;
	}

}
