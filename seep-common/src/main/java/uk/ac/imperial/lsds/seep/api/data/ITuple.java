package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.seep.errors.SchemaException;
import uk.ac.imperial.lsds.seep.util.Utils;


public class ITuple {
	
	protected final Schema schema;
	private int streamId;
	// attribute name - offset
	protected Map<String, Integer> mapFieldToOffset;
	// attribute index - offset
	protected int[] mapIdxToOffset;
	// attribute name - index in mapIdxToOffset
	protected Map<String, Integer> mapFieldToIdx;
	private ByteBuffer wrapper;
	private byte[] data;
	
	public ITuple(Schema schema){
		this.schema = schema;
		mapFieldToOffset = new HashMap<>();
		mapIdxToOffset = new int[schema.names().length];
		mapFieldToIdx = new HashMap<>();
		if(! schema.isVariableSize()) {
			// This only happens once
			this.populateOffsets();
		}
	}
	
	private ITuple() {
		this.schema = null; // TODO: EmptySchema?
	}
	
	public static ITuple makeEmptyITuple() {
		return new ITuple();
	}
	
	/** FIXME: temporal solution to allow on-demand ITuple creation **/
	
	public ITuple(Schema schema, byte[] data) {
		this.schema = schema;
		mapFieldToOffset = new HashMap<>();
		mapIdxToOffset = new int[schema.names().length];
		mapFieldToIdx = new HashMap<>();
		if( ! schema.isVariableSize()) {
			this.populateOffsets();
		}
		this.data = data;
		// greedily populate offsets for lazy deserialisation
		if(schema.isVariableSize()){
			this.populateOffsets();
		}
		wrapper = ByteBuffer.wrap(data);
	}
	
	/**
	 * This method is meant to be used once, and cache the index for future accesses.
	 * It is only safe to use (in general only safe to use access by index) when schema is fixed size
	 * @param fieldName
	 * @return
	 */
	public int getIndexFor(String fieldName) {
		return mapFieldToIdx.get(fieldName);
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public void setStreamId(int streamId){
		this.streamId = streamId;
	}
	
	public int getStreamId(){
		return streamId;
	}
	
	public void setData(byte[] data){
		this.data = data;
		// greedily populate offsets for lazy deserialisation
		if(schema.isVariableSize()){
			this.populateOffsets();
		}
		wrapper = ByteBuffer.wrap(data);
	}
	
	public byte[] getData(){
		return data;
	}
	
	/** Consider moving these fields to a different interface to not expose the rest to users? **/
	
	public byte getByte(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.BYTE +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.get();
	}
	
	public byte getByte(int idx) {
		int offset = mapIdxToOffset[idx];
		wrapper.position(offset);
		return wrapper.get();
	}
	
	public short getShort(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.SHORT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.SHORT +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getShort();
	}
	
	public short getShort(int idx) {
		int offset = mapIdxToOffset[idx];
		wrapper.position(offset);
		return wrapper.getShort();
	}
	
	public int getInt(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.INT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.INT+"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getInt();
	}
	
	public int getInt(int idx) {
		int offset = mapIdxToOffset[idx];
		wrapper.position(offset);
		return wrapper.getInt();
	}
	
	public long getLong(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.LONG)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.LONG +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getLong();
	}
	
	public long getLong(int idx) {
		int offset = mapIdxToOffset[idx];
		wrapper.position(offset);
		return wrapper.getLong();
	}
	
	public String getString(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		else if(! schema.typeCheck(fieldName, Type.STRING)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.STRING +"' with name '"+fieldName+"'");
		}

		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		String str = (String) Type.STRING.read(wrapper);
		return str;
	}

	public float getFloat(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.FLOAT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '" + Type.FLOAT + "' with name '"+fieldName+"'");
		}

		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getFloat();
	}
	
	public float getFloat(int idx) {
		int offset = mapIdxToOffset[idx];
		wrapper.position(offset);
		return wrapper.getFloat();
	}

	public double getDouble(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.DOUBLE)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '" + Type.DOUBLE + "' with name '"+fieldName+"'");
		}

		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getDouble();
	}
	
	public double getDouble(int idx) {
		int offset = mapIdxToOffset[idx];
		wrapper.position(offset);
		return wrapper.getDouble();
	}
	
	public Object get(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		Object o = null;
		Type t = schema.getField(fieldName);
		if(t.equals(Type.BYTE)){
			o = wrapper.get();
		} else if(t.equals(Type.INT)){
			o = wrapper.getInt();
		} else if(t.equals(Type.SHORT)){
			o = wrapper.getShort();
		} else if(t.equals(Type.LONG)){
			o = wrapper.getLong();
		} else if(t.equals(Type.STRING)){
			o = Type.STRING.read(wrapper);
		} else if(t.equals(Type.FLOAT)){
			o = wrapper.getFloat();
		} else if(t.equals(Type.DOUBLE)){
			o = wrapper.getDouble();
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
			mapFieldToIdx.put(names[i], i); // assign idx in order
			mapIdxToOffset[i] = offset; // store offset in idx
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
