package uk.ac.imperial.lsds.seep.api.data;

import java.nio.charset.Charset;

import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.SchemaParser;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.errors.SchemaException;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;

public class CSVParser implements SchemaParser {
	private String encoding = Charset.defaultCharset().name();
	private static CSVParser instance = null;
	Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();

	private CSVParser(){}
	
	public static CSVParser getInstance(){
		if(instance == null){
			instance = new CSVParser();
		}
		return instance;
	}
	
	public byte[] bytesFromString(String textRecord) {
		String[] parts = textRecord.split(",");
		Type[] getTypes = schema.fields();
		Object[] values = new Object[getTypes.length];
		for(int index  = 0; index < getTypes.length; index++) {
			Type t = getTypes[index];
			if (t.equals(Type.BYTE)){
				values[index] = new Byte(parts[index]);
			}
			else if(t.equals(Type.SHORT)){
				values[index] = new Short(parts[index]);
			}
			else if(t.equals(Type.INT)){
				values[index] = new Integer(parts[index]);
			}
			else if(t.equals(Type.LONG)){
				values[index] = new Long(parts[index]);
			}
			else if(t.equals(Type.STRING)){
				values[index] = parts[index];
			}
			else if(t.equals(Type.FLOAT)){
				values[index] = new Float(parts[index]);
			}
			else if(t.equals(Type.DOUBLE)){
				values[index] = new Double(parts[index]);
			}
			else {
				throw new SchemaException("Unknown type in schema");				
			}
		}
		return OTuple.create(schema, schema.names(), values);
	}
	
	public String stringFromBytes(byte[] binaryRecord) {
		ITuple data = new ITuple(schema, binaryRecord);
		String[] fields = schema.names();
		String returnValue = "";
		Type[] getTypes = schema.fields();
		for (int index = 0; index < fields.length; index++) {
			if (index > 0) {
				returnValue = returnValue + ",";
			}
			Type t = getTypes[index];
			if (t.equals(Type.BYTE)){
				returnValue += data.getByte(fields[index]);
			}
			else if(t.equals(Type.SHORT)){
				returnValue += data.getShort(fields[index]);
			}
			else if(t.equals(Type.INT)){
				returnValue += data.getInt(fields[index]);
			}
			else if(t.equals(Type.LONG)){
				returnValue += data.getLong(fields[index]);
			}
			else if(t.equals(Type.STRING)){
				returnValue += data.getString(fields[index]);
			}
			else if(t.equals(Type.FLOAT)){
				returnValue += data.getFloat(fields[index]);
			}
			else if(t.equals(Type.DOUBLE)){
				returnValue += data.getDouble(fields[index]);
			}
			else {
				throw new SchemaException("Unknown type in schema");				
			}			
		}
		return returnValue;
	}
	
	public String getCharsetName() {
		return encoding;
	}
	
	public void setCharset(String newencoding) {
		encoding = newencoding;
	}
	
	public void setSchema(Schema newSchema) {
		schema = newSchema;
	}
}



