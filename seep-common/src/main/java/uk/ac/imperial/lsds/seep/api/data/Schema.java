package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.errors.SchemaException;
import uk.ac.imperial.lsds.seep.util.Utils;

public class Schema {
	
	private final int schemaId;
	private final Type[] fields;
	private final String[] names;
	private final boolean variableSize;
	// Maps fieldName to fieldPosition (fields are ordered in a certain way)
	private Map<String, Integer> mapFieldNameToFieldPosition = new HashMap<>();
	private SchemaParser parser = DefaultParser.getInstance();
	
	private Schema(int schemaId, Type[] fields, String[] names){
		this.schemaId = schemaId;
		this.fields = fields;
		this.names = names;
		boolean variableSizeSchema = false;
		for(int i = 0; i < names.length; i++){
			mapFieldNameToFieldPosition.put(names[i], i);
			if(fields[i].isVariableSize()) variableSizeSchema = true;
		}
		this.variableSize = variableSizeSchema;
	}
	
	public boolean isVariableSize(){
		return this.variableSize;
	}
	
	public boolean hasField(String fieldName){
		return mapFieldNameToFieldPosition.containsKey(fieldName);
	}
	
	public boolean typeCheck(String fieldName, Type type){
		//Compare them as objects will be always false - compare their string representations instead ?
		return fields[mapFieldNameToFieldPosition.get(fieldName)].toString().compareTo(type.toString()) == 0 ;
	}
	
	public boolean typeCheck(String fieldName, Object o){
		Type t = fields[mapFieldNameToFieldPosition.get(fieldName)];
		if (t.equals(Type.BYTE)){
			if(o instanceof Byte){
				return true;
			}
		}
		else if(t.equals(Type.SHORT)){
			if(o instanceof Short){
				return true;
			}
		}
		else if(t.equals(Type.INT)){
			if(o instanceof Integer){
				return true;
			}
		}
		else if(t.equals(Type.LONG)){
			if(o instanceof Long){
				return true;
			}
		}
		else if(t.equals(Type.STRING)){
			if(o instanceof String){
				return true;
			}
		}
		else if(t.equals(Type.FLOAT)){
			if(o instanceof Float){
				return true;
			}
		}
		else if(t.equals(Type.DOUBLE)){
			if(o instanceof Double){
				return true;
			}
		}
		else if(t.equals(Type.BYTES)) {
			if (o instanceof byte[]) {
				return true;
			}
		}
		return false;
	}
	
	public int getFieldPosition(String fieldName){
		if(mapFieldNameToFieldPosition.containsKey(fieldName)){
			return mapFieldNameToFieldPosition.get(fieldName);
		}
		return -1;
	}
	
	public int schemaId(){
		return schemaId;
	}
	
	public Type[] fields(){
		return fields;
	}
	
	public String[] names(){
		return names;
	}
	
	public Type getField(String name){
		if(!mapFieldNameToFieldPosition.containsKey(name)){
			System.out.println("ERROR");
			System.exit(0);
		}
		return fields[mapFieldNameToFieldPosition.get(name)];
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SchemaID: "+schemaId);
		sb.append(Utils.NL);
		for(int i = 0; i < names.length; i++){
			sb.append("Field: "+names[i]+" : "+fields[i].toString());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}
	
	public static class SchemaBuilder {
		
		// Only one instance is safe as schemaId is handled internally automatically
		private static SchemaBuilder instance = null;
		
		private byte schemaId;
		private List<Type> fields = new ArrayList<Type>();
		private List<String> names = new ArrayList<String>();
		
		private SchemaBuilder(){}
		
		public static SchemaBuilder getInstance(){
			if(instance == null){
				instance = new SchemaBuilder();
			}
			return instance;
		}
		
		public SchemaBuilder newField(Type type, String name){
			// safety checks
			if(names.contains(name)){
				throw new SchemaException("Schema already contains a field with name: "+ name);
			}
			this.fields.add(type);
			this.names.add(name);
			return this;
		}
		
		public Schema build(){
			// Sanity check
			if(! (fields.size() == names.size())){
				throw new SchemaException("Name-Field Missmatch - Each Type should be mapped to exactly one Name");
			}
			Type[] f = new Type[fields.size()];
			f = fields.toArray(f);
			String[] s = new String[names.size()];
			s = names.toArray(s);
			Schema toReturn = new Schema(schemaId, f, s);
			schemaId++; // always increasing schemaId to ensure unique id
			this.fields.clear();
			this.names.clear();
			return toReturn;
		}
	}
	
	/**
	 * Empty constructor for Kryo serialization
	 */
	public Schema() {
		this.schemaId = 0;
		this.fields = null;
		this.names = null;
		this.variableSize = false;
	}

	/**
	 * Will return an array of default values that follow this schema
	 * @return
	 */
	public Object[] defaultValues() {
		Object[] values = new Object[fields.length];
		for(int i = 0; i < values.length; i++) {
			values[i] = fields[i].defaultValue();
		}
		return values;
	}
	
	public Object[] randomValues() {
		Object[] values = new Object[fields.length];
		for(int i = 0; i < values.length; i++) {
			values[i] = fields[i].randomValue();
		}
		return values;
	}
	
	public SchemaParser getSchemaParser() {
		return parser;
	}
	
	public void SchemaParser(SchemaParser newparser) {
		parser = newparser;
	}
	
	private static class DefaultParser implements SchemaParser {
		private String encoding = Charset.defaultCharset().name();
		private static DefaultParser instance = null;		

		private DefaultParser(){}
		
		public static DefaultParser getInstance(){
			if(instance == null){
				instance = new DefaultParser();
			}
			return instance;
		}
		
		public byte[] bytesFromString(String textRecord) {
			byte[] byteline = textRecord.getBytes(Charset.forName(encoding));
			ByteBuffer b = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) + byteline.length);
			b.putInt(byteline.length);
			b.put(byteline);
			return b.array();
		}
		
		public String stringFromBytes(byte[] binaryRecord) {
			ByteBuffer wrapper = ByteBuffer.allocate(binaryRecord.length - (Integer.SIZE/Byte.SIZE));
			wrapper.put(binaryRecord, (Integer.SIZE/Byte.SIZE), binaryRecord.length - (Integer.SIZE/Byte.SIZE));
			return new String(wrapper.array());
		}
		
		public String getCharsetName() {
			return encoding;
		}
		
		public void setCharset(String newencoding) {
			encoding = newencoding;
		}
	}

	public int sizeOfTuple() {
		int totalSize = 0;
		for(int i = 0; i < fields.length; i++) {
			totalSize = totalSize + fields[i].sizeOf(new Object());
		}
		return totalSize;
	}
	
}
