package uk.ac.imperial.lsds.seep.api.data;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Random;

import uk.ac.imperial.lsds.seep.errors.SchemaException;

public abstract class Type {
	
	public final static int SIZE_OVERHEAD = Integer.BYTES;
	
	public abstract String toString();
	public abstract void write(ByteBuffer buffer, Object o);
	public abstract Object read(ByteBuffer buffer);
	public abstract int sizeOf(Object o);
	public abstract boolean isVariableSize();
	public abstract Object defaultValue();
	public abstract Object randomValue();
	private static Random rnd = new Random();
	
	public boolean equals(Type t) {
		return this.toString().equals(t.toString());
	}
	
	public enum JavaType{
		BYTE, SHORT, INT, LONG, STRING, BYTES, FLOAT, DOUBLE
	}
	
	public static final Type BYTE = new Type() {
		
		public String toString(){
			return "BYTE";
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			buffer.put((byte)o);
		}

		@Override
		public Object read(ByteBuffer buffer) {
			Object o = buffer.get();
			return o;
		}

		@Override
		public int sizeOf(Object o) {
			return Byte.BYTES;
		}

		@Override
		public boolean isVariableSize() {
			return false;
		}
		
		@Override
		public Object defaultValue() {
			return (byte)0;
		}
		
		@Override
		public Object randomValue() {
			return (byte)(rnd.nextInt());
		}
		
	};
	
	public static final Type SHORT = new Type() {
		
		public String toString(){
			return "SHORT";
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			buffer.putShort((short)o);
		}

		@Override
		public Object read(ByteBuffer buffer) {
			Object o = buffer.getShort();
			return o;
		}

		@Override
		public int sizeOf(Object o) {
			return Short.BYTES;
		}

		@Override
		public boolean isVariableSize() {
			return false;
		}
		
		@Override
		public Object defaultValue() {
			return (short)0;
		}
		
		@Override
		public Object randomValue() {
			return (short)(rnd.nextInt());
		}
	};
	
	public static final Type INT = new Type() {
		
		public String toString(){
			return "INT";
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			buffer.putInt((int)o);
		}

		@Override
		public Object read(ByteBuffer buffer) {
			return buffer.getInt();
			
		}

		@Override
		public int sizeOf(Object o) {
			return Integer.BYTES;
		}

		@Override
		public boolean isVariableSize() {
			return false;
		}
		
		@Override
		public Object defaultValue() {
			return 0;
		}
		
		@Override
		public Object randomValue() {
			return (int)(rnd.nextInt());
		}
	};
	
	public static final Type LONG = new Type() {
		
		public String toString(){
			return "LONG";
		}


		@Override
		public Object read(ByteBuffer buffer) {
			return buffer.getLong();
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			buffer.putLong((long)o);
		}

		@Override
		public int sizeOf(Object o) {
			return Long.BYTES;
		}


		@Override
		public boolean isVariableSize() {
			return false;
		}
		
		@Override
		public Object defaultValue() {
			return (long)0L;
		}
		@Override
		public Object randomValue() {
			return (long)(rnd.nextLong());
		}
	};
	
	public static final Type STRING = new Type(){
		
		public String toString(){
			return "STRING";
		}

		@Override
		public Object read(ByteBuffer buffer) {
			int length = buffer.getInt();
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            String str = null;
            try {
				str = new String(bytes, "UTF8");
			} 
            catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            return str;
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			byte[] bytes = null;
			try {
				bytes = ((String)o).getBytes("UTF8");
			} 
			catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            if (bytes.length > Integer.MAX_VALUE)
                throw new SchemaException("A string or charsequence cannot be longer than Integer.MAX_VALUE");
            buffer.putInt((int) bytes.length);
            buffer.put(bytes);
		}

		@Override
		public int sizeOf(Object o) {
			return Integer.BYTES + uk.ac.imperial.lsds.seep.util.Utils.utf8Length((String)o);
		}

		@Override
		public boolean isVariableSize() {
			return true;
		}
		
		@Override
		public Object defaultValue() {
			return "string";
		}
		
		@Override
		public Object randomValue() {
			return new Integer((rnd.nextInt())).toString();
		}
	};
	
	public static final Type SHORTSTRING = new Type(){

		private final int maxSize = 64; // bytes
		
		@Override
		public String toString() {
			return "SHORTSTRING";
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			// TODO;
			
		}

		@Override
		public Object read(ByteBuffer buffer) {
			// TODO:
			return null;
		}

		@Override
		public int sizeOf(Object o) {
			return maxSize;
		}

		@Override
		public boolean isVariableSize() {
			return false;
		}
		
		@Override
		public Object defaultValue() {
			return "string";
		}
		
		@Override
		public Object randomValue() {
			return new Integer((rnd.nextInt())).toString();
		}
		
	};
	
	public static final Type BYTES = new Type() {
		
		public String toString(){
			return "BYTES";
		}

		@Override
		public Object read(ByteBuffer buffer) {
//			int size = buffer.getInt();
//            ByteBuffer val = buffer.slice();
//            val.limit(size);
//            buffer.position(buffer.position() + size);
//            return val;
			int length = buffer.getInt();
			byte[] bytes = new byte[length];
			buffer.get(bytes);
			return bytes;
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
//			ByteBuffer arg = (ByteBuffer) o;
//            int pos = arg.position();
//            buffer.putInt(arg.remaining());
//            buffer.put(arg);
//            arg.position(pos);
			byte[] bytes = (byte[]) o;
			buffer.putInt(bytes.length);
			buffer.put(bytes);
		}

		@Override
		public int sizeOf(Object o) {
//			ByteBuffer buffer = (ByteBuffer) o;
//            return Integer.BYTES + buffer.remaining();
			byte[] bytes = (byte[]) o;
			return Integer.BYTES + bytes.length;
		}

		@Override
		public boolean isVariableSize() {
			return true;
		}
		
		@Override
		public Object defaultValue() {
			return new byte[1];
		}
		
		@Override
		public Object randomValue() {
			return new byte[1];
		}
	};
	
	public static final Type FLOAT = new Type() {
		
		public String toString(){
			return "FLOAT";
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			buffer.putFloat((float)o);
		}

		@Override
		public Object read(ByteBuffer buffer) {
			return buffer.getFloat();
			
		}

		@Override
		public int sizeOf(Object o) {
			return Float.BYTES;
		}

		@Override
		public boolean isVariableSize() {
			return false;
		}
		
		@Override
		public Object defaultValue() {
			return 0.0f;
		}
		
		@Override
		public Object randomValue() {
			return (float)(rnd.nextFloat());
		}
	};

	public static final Type DOUBLE = new Type() {

		public String toString(){
			return "DOUBLE";
		}

		@Override
		public void write(ByteBuffer buffer, Object o) {
			buffer.putDouble((double)o);
		}

		@Override
		public Object read(ByteBuffer buffer) {
			return buffer.getDouble();

		}

		@Override
		public int sizeOf(Object o) {
			return Double.BYTES;
		}

		@Override
		public boolean isVariableSize() {
			return false;
		}

		@Override
		public Object defaultValue() {
			return 0.0d;
		}

		@Override
		public Object randomValue() {
			return (double)(rnd.nextDouble());
		}
	};
}
