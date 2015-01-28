package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;


public class Array extends Type {

	private final Type type;

    public Array(Type type) {
        this.type = type;
    }
    
    public Type type(){
    	return type;
    }
	
	@Override
	public String toString() {
		return "@["+type.toString()+"]"; // for example: @[INT]
	}

	@Override
	public void write(ByteBuffer buffer, Object o) {
		Object[] objs = (Object[]) o;
        int size = objs.length;
        buffer.putInt(size);
        for (int i = 0; i < size; i++)
            type.write(buffer, objs[i]);
	}

	@Override
	public Object[] read(ByteBuffer buffer) {
		int size = buffer.getInt();
        Object[] objs = new Object[size];
        for (int i = 0; i < size; i++)
            objs[i] = type.read(buffer);
        return objs;
	}

	@Override
	public int sizeOf(Object o) {
		Object[] objs = (Object[]) o;
        int size = Integer.BYTES;
        for (int i = 0; i < objs.length; i++)
            size += type.sizeOf(objs[i]);
        return size;
	}

	@Override
	public boolean isVariableSize() {
		return type.isVariableSize();
	}

}
