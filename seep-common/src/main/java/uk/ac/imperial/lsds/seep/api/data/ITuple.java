package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.seep.util.Utils;

/**
 * Tuple is a class that contains data in a byte[] form and an accompanying schema. Data will be deserialized lazily
 * which will reduce overhead in many cases. All overhead is then pushed to the application, not part of the system.
 * @author ra
 */

public class ITuple implements DataItem{

	private boolean consumed = false;
	
	private final Schema schema;
	private int streamId;
	private Map<String, Integer> mapFieldToOffset;
	private ByteBuffer wrapper;
	private byte[] data;
	
	private List<ITuple> dataCol;
	private Iterator<ITuple> it;
	
	public ITuple(Schema schema){
		this.schema = schema;
		mapFieldToOffset = new HashMap<>();
		if(! schema.isVariableSize()){
			// This only happens once
			this.populateOffsets();
		}
	}

	@Override
	public ITuple consume() {
		if(!consumed){
			consumed = true;
			return this;
		}
		else{
			consumed = true;
			return null;
		}
	}
	
	public void setStreamId(int streamId){
		this.streamId = streamId;
	}
	
	public int getStreamId(){
		return streamId;
	}
	
	public void setData(byte[] data){
		consumed = false;
		this.data = data;
		// greedily populate offsets for lazy deserialisation
		if(schema.isVariableSize()){
			this.populateOffsets();
		}
		wrapper = ByteBuffer.wrap(data);
	}
	
	public void setData(List<byte[]> dataCol){
		consumed = false;
		this.dataCol = new ArrayList<>();
		for(byte[] el : dataCol){
			ITuple i = new ITuple(schema);
			i.setData(el);
			this.dataCol.add(i);
		}
		it = this.dataCol.iterator();
	}
	
	public boolean hasNext(){
		return it.hasNext();
	}
	
	public ITuple next(){
		return it.next();
	}
	
	public void remove(){
		it.remove();
	}
	
	public byte[] getData(){
		return data;
	}
	
	/** Consider moving these fields to a different interface to not expose the rest to users? **/
	
	public byte getByte(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.get();
	}
	
	public short getShort(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getShort();
	}
	
	public int getInt(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getInt();
	}
	
	public long getLong(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getLong();
	}
	
	public String getString(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		else if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}

		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		String str = (String) Type.STRING.read(wrapper);
		return str;
	}
	
	public Object get(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		Object o = null;
		Type t = schema.getField(fieldName);
		if(t == Type.BYTE){
			o = wrapper.get();
		} else if(t == Type.INT){
			o = wrapper.getInt();
		} else if(t == Type.SHORT){
			o = wrapper.getShort();
		} else if(t == Type.LONG){
			o = wrapper.getLong();
		} else if(t == Type.STRING){
			o = Type.STRING.read(wrapper);
		}
		return o;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(String fieldName : schema.names()){
			Object o = this.get(fieldName);
			sb.append(fieldName+": "+o.toString());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}
	
	private void populateOffsets(){
		Type[] fields = schema.fields();
		String[] names = schema.names();
		int offset = 0;
		for(int i = 0; i < fields.length; i++){
			Type t = fields[i];
			mapFieldToOffset.put(names[i], offset);
			if(! t.isVariableSize()){
				// if not variable we just get the size of the Type
				offset = offset + t.sizeOf(null);
			}
			else {
				// if variable we need to read the size from the current offset
				ByteBuffer temp = ByteBuffer.wrap(data);
				temp.position(offset);
				int size = temp.getInt();
				offset = offset + size + Type.SIZE_OVERHEAD;
			}
		}
	}
	
	public String printOffsets(){
		StringBuffer sb = new StringBuffer();
		for(Entry<String, Integer> entry : mapFieldToOffset.entrySet()){
			sb.append("Field: "+entry.getKey()+" Offset: "+entry.getValue());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}
	
}