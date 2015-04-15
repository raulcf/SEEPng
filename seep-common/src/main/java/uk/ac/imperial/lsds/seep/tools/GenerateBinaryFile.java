package uk.ac.imperial.lsds.seep.tools;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.api.data.Type;

public class GenerateBinaryFile {

	private static int FILE_SIZE = 10; // 10MB
	private static int BATCH_SIZE = 10;
	
	public static void main(String[] args){
		OptionParser parser = new OptionParser();
		parser.accepts("schema", "Schema fields").withRequiredArg().required();
		parser.accepts("types", "Seed values").withRequiredArg().required();
		parser.accepts("values", "Seed values").withRequiredArg().required();
		parser.accepts("output", "Absolute path to output generated file").withRequiredArg().required();
		parser.accepts("filesize", "Desired file size in MB").withRequiredArg();
		OptionSet os = parser.parse(args);
		Properties p = asProperties(os);
		
		Schema s = createSchema(p.getProperty("schema"), p.getProperty("types"));
		
		int fileSize = p.containsKey("filesize") ? Integer.parseInt((String) p.getProperty("filesize")) : FILE_SIZE;
		String path = p.getProperty("output");
		createFile(s, p.getProperty("output"), fileSize);
		System.out.println("-> Created file: "+path+" of size: "+fileSize);
	}
	
	// TODO: just fill with ints for now...
	public static void createFile(Schema s, String path, int targetSize){
		File output = new File(path);
		int currentSize = 0;
		BufferedOutputStream bos = null;
		DataOutputStream dos = null;
		try {
			bos = new BufferedOutputStream(new FileOutputStream(output));
			dos = new DataOutputStream(bos);
			Object[] values = new Object[s.names().length];
			while((currentSize/1000000) < targetSize){
				for(int i = 0; i < values.length; i++){
					values[i] = values[i] == null ? 2 : (int)values[i] + 2;
				}
				byte[] data = OTuple.create(s, s.names(), values);
				int batchSize = data.length * BATCH_SIZE + TupleInfo.TUPLE_SIZE_OVERHEAD * BATCH_SIZE;
				dos.write(0); // control byte
				dos.writeInt(BATCH_SIZE); // num tuples in batch
				dos.writeInt(batchSize); // total batch size
				for(int i = 0; i < BATCH_SIZE; i++){
					dos.writeInt(data.length);
					dos.write(data);
					currentSize = currentSize + data.length + 4;
				}
				currentSize = currentSize + 1 + 4 + 4;
			}
			dos.flush();
			dos.close();
			bos.flush();
			bos.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static Schema createSchema(String schema, String types){
		String[] sTokens = schema.split(",");
		String[] tTokens = types.split(",");
		System.out.println("#Fields: "+sTokens.length+" #Types: "+tTokens.length);
		if(sTokens.length != tTokens.length 
				|| sTokens.length == 0 
				|| tTokens.length == 0){
			System.out.println("schema, types  do not match (size) or is 0");
			System.exit(0);
		}
		
		SchemaBuilder sb = SchemaBuilder.getInstance();
		for(int i = 0; i < sTokens.length; i++){
			Type type = getType(tTokens[i]);
			sb.newField(type, sTokens[i]);
		}
		
		return sb.build();
	}
	
	private static Type getType(String type){
		switch(type){
			case "int":
				return Type.INT;
			case "long":
				return Type.LONG;
			case "short":
				return Type.SHORT;
			case "byte":
				return Type.BYTE;
			case "bytes":
				return Type.BYTES;
			case "shortstring":
				return Type.SHORTSTRING;
			case "string":
				return Type.STRING;
			default:
				System.out.println("Non-recognized type");
				System.exit(0);
				return null;
		}
	}
	
	private static Properties asProperties(OptionSet options) {
        Properties properties = new Properties();
        for ( Entry<OptionSpec<?>, List<?>> entry : options.asMap().entrySet() ) {
            OptionSpec<?> spec = entry.getKey();
            String key = asPropertyKey(spec);
            String value = asPropertyValue(entry.getValue(), options.has(spec));
            properties.setProperty(key, value);
        }
        return properties;
    }
	
	private static String asPropertyKey(OptionSpec<?> spec) {
        List<String> flags = (List<String>) spec.options();
        for ( String flag : flags )
            if ( 1 < flag.length() )
                return flag;
        throw new IllegalArgumentException( "No usable non-short flag: " + flags );
    }

    private static String asPropertyValue( List<?> values, boolean present ) {
        // Simple flags have no values; treat presence/absence as true/false
    	String value = "";
    	if(values.isEmpty()){
    		return String.valueOf(present);
    	}
    	else{
    		for(int i = 0; i < values.size(); i++){
    			if(i != 0){
    				value.concat(",");
    			}
    			value = value.concat(String.valueOf(values.get(i)));
    		}
    	}
    	return value;
    }
	
}
