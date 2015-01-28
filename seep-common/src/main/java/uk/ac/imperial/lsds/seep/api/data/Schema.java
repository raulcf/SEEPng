package uk.ac.imperial.lsds.seep.api.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.util.Utils;

public class Schema {
	
	private final int schemaId;
	private final Type[] fields;
	private final String[] names;
	private final boolean variableSize;
	// Maps fieldName to fieldPosition (fields are ordered in a certain way)
	private Map<String, Integer> mapFieldNameToFieldPosition = new HashMap<>();
		
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
		return fields[mapFieldNameToFieldPosition.get(fieldName)].equals(type);
	}
	
	public boolean typeCheck(String fieldName, Object o){
		Type t = fields[mapFieldNameToFieldPosition.get(fieldName)];
		if (t.toString().equals(Type.BYTE.toString())){
			if(o instanceof Byte){
				return true;
			}
		}
		else if(t.toString().equals(Type.SHORT.toString())){
			if(o instanceof Short){
				return true;
			}
		}
		else if(t.toString().equals(Type.INT.toString())){
			if(o instanceof Integer){
				return true;
			}
		}
		else if(t.toString().equals(Type.LONG.toString())){
			if(o instanceof Long){
				return true;
			}
		}
		else if(t.toString().equals(Type.STRING.toString())){
			if(o instanceof String){
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
				// TODO: throw error
			}
			this.fields.add(type);
			this.names.add(name);
			return this;
		}
		
		public Schema build(){
			// Sanity check
			if(! (fields.size() == names.size())){
				// TODO: throw error
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
	
}
