package uk.ac.imperial.lsds.java2sdg.codegenerator;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.VariableRepr;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.util.Utils;


public class SeepOperatorNewTemplate {

	private static Logger LOG = LoggerFactory.getLogger(SeepOperatorNewTemplate.class.getCanonicalName());
	
//	public static String getCodeForMultiOp(Map<Integer,TaskElementRepr> tes){
//		StringBuilder sb = new StringBuilder();
//		sb.append("{"); // open block
//		sb.append(_getCodeForMultiOp(tes));
//		sb.append("}"); // close block
//		return sb.toString();
//	}
//	
//	private static String _getCodeForMultiOp(Map<Integer,TaskElementRepr> tes){
//		StringBuilder sb = new StringBuilder();
//		
//		// Build IF block and insert code for first TE
//		TaskElementRepr firstTE = tes.remove(0);
//		String initIFBlock = getInitIFBlock();
//		sb.append(initIFBlock);
//		sb.append(_getCodeForSingleOp(firstTE));
//		sb.append("}");
//		// Once the IF block has started, we just complete it with else clauses
//		int branchId = 1; // 0 is used for firstTE
//		for(Map.Entry<Integer, TaskElementRepr> te : tes.entrySet()){
//			sb.append("else if(branchId == "+branchId+"){");
//			branchId++;
//			sb.append(_getCodeForSingleOp(te.getValue()));
//			sb.append("}");
//		}
//		return sb.toString();
//	}
//	
//	private static String getInitIFBlock(){
//		StringBuilder sb = new StringBuilder();
//		String unbox = getUnboxCode("java.lang.Integer", "branchId");
//		sb.append(unbox);
//		sb.append("if(branchId == 0){");
//		return sb.toString();
//	}
	
	public static String getCodeForSingleOp(TaskElementRepr te){
		StringBuilder sb = new StringBuilder();
		sb.append("{"); // open block
		sb.append(_getCodeForSingleOp(te));
		sb.append("}"); // close block
		return sb.toString();
	}
	
	private static String _getCodeForSingleOp(TaskElementRepr te){
		// Extract and synthesize code for getting the right variables. This is constant
		List<VariableRepr> localVars = te.getInputVariables();
		String header = getCodeToLocalVars(localVars);
		
		// Append TE code
		List<String> code = te.getCode();
		
		// Get code to send downstream. Append branching id always.
		List<VariableRepr> varsToStream = te.getOutputVariables();
		
		String footer = "";
		if(varsToStream != null && !varsToStream.isEmpty()){
			//Simple Schema - inputVars check
			if(te.getOutputSchema().fields().length != varsToStream.size()){
				LOG.error("OutputSchema and StreamVariables size missmatch!!");
				LOG.error("Schema {}", te.getOutputSchema());
				LOG.error("Variables {}", varsToStream);
				System.exit(0);
			}
			
			footer = getCodeToSend(varsToStream, te.getOutputSchema());
		}
		/*
		 * Merge ALL Code blocks together
		 */
		StringBuilder sb = new StringBuilder();
		sb.append(header);
		for(String codeBlock : code)
			sb.append(codeBlock);
		sb.append(footer);
		 
		return sb.toString();
	}
	
	private static String getCodeToLocalVars(List<VariableRepr> localVars){
		StringBuilder code = new StringBuilder();
		for(VariableRepr v : localVars){
			String unbox = getUnboxCode(v.getType(), v.getName());
			code.append(unbox);
		}
		return code.toString();
	}
	
	/*
	 * Added Janino Primitive Type Mappings
	 * Maybe also add float and short?
	 */
	private static String getUnboxCode(String type, String name){
		StringBuilder sb = new StringBuilder();
		String varType_stmt1 = null;
		String unboxVarMethodName_stmt1 = null;
		String varType_stmt2 = null;
		String unboxVarMethodName_stmt2 = null;
		LOG.debug("Unbox INPUT Code TYPE: {} ",type);
		if(type.equals("java.lang.Integer") || type.equals("int")){
			varType_stmt1 = "int";
//			varType_stmt2 = "int";
			unboxVarMethodName_stmt1 = " = $1.getInt(";
//			unboxVarMethodName_stmt2 = ".intValue();";
		}
		else if(type.equals("java.lang.String") || type.equals("String") || type.equals("byte")){
			varType_stmt1 = "String";
			unboxVarMethodName_stmt1 = " = $1.getString(";
		}
		else if(type.equals("java.lang.Long") || type.equals("long") ){
			varType_stmt1 = "long";
//			varType_stmt2 = "long";
			unboxVarMethodName_stmt1 = " = $1.getLong(";
//			unboxVarMethodName_stmt2 = ".longValue();";
		}
		else if(type.equals("java.lang.Double") || type.equals("double") ){
			varType_stmt1 = "Double";
			varType_stmt2 = "double";
			unboxVarMethodName_stmt1 = " = $1.getDouble(";
			unboxVarMethodName_stmt2 = ".doubleValue();";
		}
		else if(type.equals("java.lang.Character") || type.equals("char") ){
			varType_stmt1 = "Character";
			varType_stmt2 = "char";
			unboxVarMethodName_stmt1 = " = $1.getChar(";
			unboxVarMethodName_stmt2 = ".charValue();";
		}
		else if(type.equals("java.lang.Boolean") || type.equals("boolean")){
			varType_stmt1 = "Boolean";
			varType_stmt2 = "boolean";
			unboxVarMethodName_stmt1 = " = $1.getBoolean(";
			unboxVarMethodName_stmt2 = ".booleanValue();";
		}
		else{
			LOG.error("getUnBoxCode unknown variable: {} type: {}", name, type );
			System.exit(0);
		}
		// Build actual lines
		sb.append(varType_stmt1+" "+name+" "+unboxVarMethodName_stmt1+"\""+name+"\""+");\n");
		if(varType_stmt2 != null){ // It there is such statement 2
			sb.append(varType_stmt2+" "+name+" = "+name+""+unboxVarMethodName_stmt2);
		}
		return sb.toString();
	}
	
	/*
	 * TODO: Need a way to ensure variables and schema ORDER and SIZE do match!
	 * 
	 */
	private static String getCodeToSend(List<VariableRepr> varsToStream, Schema sc ){
		StringBuffer vars = new StringBuffer();
		for(int i = 0; i<varsToStream.size(); i++){
			VariableRepr v = varsToStream.get(i);
			String boxCode = getBoxCode(v.getType(), v.getName());
			LOG.debug("Box OUTPUT Code var {}  ", v.getName());
			if(i == (varsToStream.size()-1))
				vars.append(boxCode);
			else
				vars.append(boxCode+", ");
		}
		
		StringBuffer varSchema = new StringBuffer();
		for(int i =0; i < sc.names().length; i++){
			if(i == sc.names().length-1)
				varSchema.append('"'+ sc.names()[i] +'"');
			else
				varSchema.append('"'+ sc.names()[i] +"\", ");
		}
		
		// Note that $1 is the method argument -> api
		String producedCode = Utils.NL + 
				"byte[] processedData = OTuple.create(schema, new String[]{" + varSchema.toString() +"}," +
				"new Object[]{ " +vars.toString() +" });"+ Utils.NL +
				"$2.send(processedData);"
			+ Utils.NL;
		return producedCode;
	}
	
	
	/**
	 * 
	 * Schema Code-Gen Helper Functions 
	 *
	 */
	
	public static String getSchemaInstance(Schema sc){
		StringBuffer schemaInstance = new StringBuffer();
		schemaInstance.append("Schema schema = Schema.SchemaBuilder.getInstance()");
		for(int i =0; i < sc.names().length; i++){
			schemaInstance.append(".newField(Type."+sc.getField(sc.names()[i]) + ", \""+sc.names()[i]+"\")" );
		}
		schemaInstance.append(".build();");
		return schemaInstance.toString();
	}
	
	public static String getSchemaNames(Schema sc){
		StringBuffer varSchema = new StringBuffer();
		for(int i =0; i < sc.names().length; i++){
			if(i == sc.names().length-1)
				varSchema.append('"'+ sc.names()[i] +'"');
			else
				varSchema.append('"'+ sc.names()[i] +"\", ");
		}
		return varSchema.toString();
	}
	
	public static String getSchemaVarsInit(Schema sc){
		StringBuffer varInitialisation = new StringBuffer();
		for(int i =0; i < sc.names().length; i++){
			String boxCode = getInitBoxCode(sc.getField(sc.names()[i]).toString(), sc.names()[i]);
			varInitialisation.append(boxCode + ";");
		}
		return varInitialisation.toString();
	}
	
	public static String getSchemaVarsBoxed(Schema sc){
		StringBuffer varInitialisation = new StringBuffer();
		for(int i =0; i < sc.names().length; i++){
			String boxCode = getBoxCode(sc.getField(sc.names()[i]).toString(), sc.names()[i]);
			varInitialisation.append(boxCode);
			if(i < (sc.names().length -1))
				varInitialisation.append(",");
		}
		return varInitialisation.toString();
	}
	
	public static String increaseSchemaVars(Schema sc){
		StringBuffer varInitialisation = new StringBuffer();
		for(int i =0; i < sc.names().length; i++){
			if ( sc.getField(sc.names()[i]).toString() == "INT" ){
				varInitialisation.append(sc.names()[i] + "=" + sc.names()[i] + "+1;");
			}
			if ( sc.getField(sc.names()[i]).toString() == "LONG" ){
				varInitialisation.append(sc.names()[i] + "=" + sc.names()[i] + "+1;");
			}
		}
		return varInitialisation.toString();
	}
	
	/*
	 * Added Janino Primitive Type Mappings
	 * Maybe also add float and short?
	 */
	private static String getBoxCode(String type, String name){
		String c = null;
		if( type.equals("java.lang.Integer") || type.equals("INT") || type.equals("int") ){
			c = "new Integer("+name+")";
		}
		else if(type.equals("java.lang.Long") ||  type.equals("LONG") || type.equals("long")){
			c = "new Long("+name+")";
		}
		else if(type.equals("java.lang.String") ||  type.equals("STRING") || type.equals("String") || type.equals("byte")){
			c = "new String("+name+")";
		}
		else{
			LOG.error("getBoxCode unknown variable: {} type: {}", name, type );
			System.exit(0);
		}
		return c;
	}
	
	
	private static String getInitBoxCode(String type, String name){
		String c = null;
		if( type.equals("java.lang.Integer") || type.equals("INT") || type.equals("int") ){
			c = "int "+ name + " =  0";
		}
		else if(type.equals("java.lang.Long") ||  type.equals("LONG") || type.equals("long")){
			c = "long "+ name + " = 0l";
		}
		else if(type.equals("java.lang.String") ||  type.equals("STRING") || type.equals("String")){
			c = "String "+ name + " = \"some text\"";
		}
		else{
			LOG.error("getBoxCode unknown variable: {} type: {}", name, type );
			System.exit(0);
		}
		return c;
	}
	
//	private static String getCodeToSend_Source(List<String> varsToStream){
//		StringBuffer vars = new StringBuffer();
//		for(int i = 0; i<varsToStream.size(); i++){
//			//FIXME: assuming always integer for debugging
//			if(i == (varsToStream.size()-1))
//				vars.append("new Integer("+varsToStream.get(i)+")");
//			else
//				vars.append("new Integer("+varsToStream.get(i)+"), ");
//		}
//		// Note that $1 is the tuple we receive -> data
//		String code = "" +
//				"DataTuple output = tuple.newTuple(new Object[] {"+vars.toString()+"});\n" +
//				"api.send(output);\n" +
//		"";
//		return code;
//	}
}