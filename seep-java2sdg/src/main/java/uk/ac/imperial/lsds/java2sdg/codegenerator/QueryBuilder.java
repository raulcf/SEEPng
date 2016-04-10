/*******************************************************************************
 * Copyright (c) 2014 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Raul Castro Fernandez - initial API and implementation
 ******************************************************************************/
package uk.ac.imperial.lsds.java2sdg.codegenerator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.bytecode.BadBytecode;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.CodeIterator;
import javassist.bytecode.MethodInfo;
import javassist.bytecode.Mnemonic;
import uk.ac.imperial.lsds.java2sdg.Util;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks2.TaskElement;
import uk.ac.imperial.lsds.java2sdg.bricks2.TaskElementNature;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.sinks.SimpleConsoleSink;

public class QueryBuilder {

	private String outputPath = Util.getProjectPath()+"/tmp/";
	private OperatorClassBuilder builder;
	private HashMap<String, TaskElement> class_opBlock = new HashMap<String, TaskElement>();
	private String composeCode;
	
	public QueryBuilder(){
		builder = new OperatorClassBuilder();
	}
	
	public void generateDummyQueryPlanDriver(SDGNode processNode){
		
		String schema = new String("Schema schema = Schema.SchemaBuilder.getInstance().newField(Type.INT, \"userId\").newField(Type.LONG, \"ts\").newField(Type.STRING, \"text\").build();\n");
		String bcode = new String ( "LogicalOperator src = queryAPI.newStatelessSource(new Src(), 0);"+
									"LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);"+
									"LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 2);"+
									"src.connectTo(processor, 0, new DataStore(schema, DataStoreType.NETWORK));"+
									"processor.connectTo(snk, 0, new DataStore(schema, DataStoreType.NETWORK));"+
									"return QueryBuilder.build();\n");
		
		String [] srcFields =  new String [] {"private boolean working = true;"};
		String srcProcess = new String ("int userId = 0; long ts = 0l; waitHere(2000); "
				+ "						while(working){ byte[] d = OTuple.create(schema, "
				+ "							new String[]{\"userId\", \"ts\", \"text\"}, new Object[]{new Integer(userId), new Long(ts),  \"some Text\"});"
										+ "$2.send(d);userId=userId+1;ts=ts+1;}");
		String srcExtraMethod = new String("private void waitHere(int time){ try { Thread.sleep((long)time);} catch (InterruptedException e) {e.printStackTrace();}}");
		
		String pcode = processNode.getBuiltCode();
		
		String [] snkFields = new String [] {"int PERIOD = 1000;", "int count = 0;", "public long time;"};
		String snkProcess = new String("count++; if(System.currentTimeMillis() - time > PERIOD){ System.out.println(\"[Sink] e/s: \"+count); count = 0;time = System.currentTimeMillis();}");
		
		QueryBuilder b = new QueryBuilder();
		CtClass srcClass = b.builder.generateSource(schema, srcFields, srcExtraMethod, srcProcess);
		CtClass processClass = b.builder.generateStatelessProcessor("Processor", schema, null, null, pcode);
		CtClass sinkClass = b.builder.generateSink(schema, snkFields, null, snkProcess);
		CtClass baseClass = b.builder.generateBase(schema + bcode);
		
		try {
			srcClass.writeFile(b.outputPath);
			processClass.writeFile(b.outputPath);
			sinkClass.writeFile(b.outputPath);
			baseClass.writeFile(b.outputPath);
			
		} 
		catch (CannotCompileException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		// Packaging All classes into jar
		b.packageToJar(b.outputPath);
	}
	
	
	public String generateQueryPlanDriver(Set<TaskElement> sdg){
//		StringBuffer compose = new StringBuffer();
//		StringBuffer opInstantiationCode = new StringBuffer();
//		StringBuffer opConnectionCode = new StringBuffer();
//		
//		int opId = 0;
//		int nodeId = 0;
//		for(TaskElement ob : sdg){
//			String opName = ob.getOpName();
//			String opFieldsName = opName+"Fields";
//			String classOpName = "C_"+opName;
//			
//			opInstantiationCode.append("ArrayList "+opFieldsName+" = new ArrayList();\n");
//			if(ob.getLocalVars() != null){
//				for(String var : ob.getLocalVars()){
//					opInstantiationCode.append(opFieldsName+".add("+"\""+var+"\""+");\n");
//				}
//			}
//			
//			TaskElementNature opType = ob.getOpType();
//			if(opType == TaskElementNature.STATELESS_SOURCE){
//				String instantiation = "Connectable "+opName+" = QueryBuilder.newStatelessSource(new "+classOpName+"(), -1, "+opFieldsName+");\n";
//				opInstantiationCode.append(instantiation);
//			}
//			else if(opType == TaskElementNature.STATEFUL_SOURCE){
//				System.out.println("Not implemented");
//				System.exit(0);
//			}
//			else if(opType == TaskElementNature.STATELESS_OPERATOR){
//				String instantiation = "Connectable "+opName+" = QueryBuilder.newStatelessOperator(new "+classOpName+"(), "+opId+", "+opFieldsName+");\n";
//				opId++;
//				opInstantiationCode.append(instantiation);
//			}
//			else if(opType == TaskElementNature.STATEFUL_OPERATOR){
//				System.out.println("Not implemented");
//				System.exit(0);
//			}
//			else if(opType == TaskElementNature.STATELESS_SINK){
//				String instantiation = "Connectable "+opName+" = QueryBuilder.newStatelessSink(new "+classOpName+"(), -2, "+opFieldsName+");\n";
//				opInstantiationCode.append(instantiation);
//			}
//			else if(opType == TaskElementNature.STATEFUL_SINK){
//				System.out.println("Not implemented");
//				System.exit(0);
//			}
//		}
//		composeCode = compose.append(opInstantiationCode).append(opConnectionCode).append("return QueryBuilder.build();\n").toString();
//		return composeCode;
		return null;
	}
	
	public void buildAndPackageQuery(){
		// Compiling operators
//		for(Map.Entry<String, TaskElement> entry : class_opBlock.entrySet()){
//			String name = entry.getKey();
//			TaskElement ob = entry.getValue();
//			TaskElementNature ot = ob.getOpType();
//			
//			System.out.println("Building: "+name);
//			System.out.println("CODE: ");
//			System.out.println(ob.getCode());
//			
//			if(ot == TaskElementNature.STATEFUL_OPERATOR || ot == TaskElementNature.STATEFUL_SINK || ot == TaskElementNature.STATEFUL_SOURCE){
//				CtClass op = builder.getStatefulOperatorClassTemplate(name);
//				System.out.println("not implemented");
//				System.exit(0);
//			}
//			else if(ot == TaskElementNature.STATELESS_OPERATOR || ot == TaskElementNature.STATELESS_SINK || ot == TaskElementNature.STATELESS_SOURCE){
//				CtClass op = builder.getStatelessOperatorClass_oneTupleAtATime(name, ob.getCode());
//				try {
//					System.out.println("NAME: "+name);
//					System.out.println(op.toString());
//					op.writeFile(outputPath);
//				}
//				catch (CannotCompileException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} 
//				catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		// Compiling base class
//		
//		CtClass composeQuery = builder.getBaseIClass(composeCode);
//		try {
//			composeQuery.writeFile(outputPath);
//		} 
//		catch (CannotCompileException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
//		catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		// Packaging into jar
//		packageToJar(outputPath);
	}
	
	private void packageToJar(String pathToDir){
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "Base");
		try {
			// Package files into .jar
			JarOutputStream target = new JarOutputStream(new FileOutputStream("java2sdg-query.jar"), manifest);
			File folderWithClasses = new File(pathToDir);
			addDirectoryToJar(folderWithClasses, target);
			target.close();
			
			// Remove temporary output directory
			for(File nestedFile: folderWithClasses.listFiles()){
				nestedFile.delete();
			}
			folderWithClasses.delete();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void addDirectoryToJar(File source, JarOutputStream target) throws IOException{
		BufferedInputStream in = null;
		try{
			if(source.isDirectory()){
				for(File nestedFile: source.listFiles()){
					//JarEntry entry = new JarEntry(nestedFile.getPath().replace("\\", "/"));
					JarEntry entry = new JarEntry(nestedFile.getName());
				    entry.setTime(nestedFile.lastModified());
				    target.putNextEntry(entry);
				    
				    in = new BufferedInputStream(new FileInputStream(nestedFile));

				    byte[] buffer = new byte[1024];
				    while(true){
				    	int count = in.read(buffer);
				    	if (count == -1)
				    		break;
				    	target.write(buffer, 0, count);
				    }
				    target.closeEntry();
				}
		    }
			else{
				System.out.println("SOURCE of files is not a directory");
				System.exit(0);
			}
		}
		finally{
			if (in != null){	
				in.close();
			}
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Testing Query Builder Functionality (simple-pipeline-stalesss-query example)...");
		
		String schema = new String("Schema schema = Schema.SchemaBuilder.getInstance().newField(Type.INT, \"userId\").newField(Type.LONG, \"ts\").newField(Type.STRING, \"text\").build();\n");
		String bcode = new String ( "LogicalOperator src = queryAPI.newStatelessSource(new Src(), 0);"+
									"LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);"+
									"LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 2);"+
									"src.connectTo(processor, 0, new DataStore(schema, DataStoreType.NETWORK));"+
									"processor.connectTo(snk, 0, new DataStore(schema, DataStoreType.NETWORK));"+
									"return QueryBuilder.build();\n");
		
		String [] srcFields =  new String [] {"private boolean working = true;"};
		String srcProcess = new String ("int userId = 0; long ts = 0l; waitHere(2000); "
				+ "						while(working){ byte[] d = OTuple.create(schema, "
				+ "							new String[]{\"userId\", \"ts\", \"text\"}, new Object[]{new Integer(userId), new Long(ts),  \"some Text\"});"
										+ "$2.send(d);userId=userId+1;ts=ts+1;}");
		String srcExtraMethod = new String("private void waitHere(int time){ try { Thread.sleep((long)time);} catch (InterruptedException e) {e.printStackTrace();}}");
		
		String pcode = new String(	"int userId = $1.getInt(\"userId\");\n" +
									"long ts = $1.getLong(\"ts\");\n" +
									"String text = $1.getString(\"text\");\n" + 
									"text = text + \"_processed\";\n" +
									"userId = userId + userId;\n" +
									"ts = ts - 1;\n"+
									"byte[] processedData = OTuple.create(schema, new String[]{\"userId\", \"ts\", \"text\"}, new Object[]{new Integer(userId), new Long(ts), new String(text)});\n" +
									"$2.send(processedData);\n");
		
		String [] snkFields = new String [] {"int PERIOD = 1000;", "int count = 0;", "public long time;"};
		String snkProcess = new String("count++; if(System.currentTimeMillis() - time > PERIOD){ System.out.println(\"[Sink] e/s: \"+count); count = 0;time = System.currentTimeMillis();}");
		
		QueryBuilder b = new QueryBuilder();
		CtClass srcClass = b.builder.generateSource(schema, srcFields, srcExtraMethod, srcProcess);
		CtClass processClass = b.builder.generateStatelessProcessor("Processor", schema, null, null, pcode);
		CtClass sinkClass = b.builder.generateSink(schema, snkFields, null, snkProcess);
		CtClass baseClass = b.builder.generateBase(schema + bcode);
		
				
		try {
//			System.out.println(baseClass.toString());
//			System.out.println(processClass.toString());

			srcClass.writeFile(b.outputPath);
			processClass.writeFile(b.outputPath);
			sinkClass.writeFile(b.outputPath);
			baseClass.writeFile(b.outputPath);
			
//			CtClass testProc =  b.builder.getCp().get("TestProcessor");
//			b.builder.getCp().insertClassPath(new ClassClassPath(testProc.getClass()));
//			
////			processClass.defrost();
//			System.out.println("MY Processor =>\n "+ processClass.getDeclaredMethod("processData").getMethodInfo());
			
//			MethodInfo minfo = testProc.getDeclaredMethod("processData").getMethodInfo();
//			MethodInfo pindo = processClass.getDeclaredMethod("processData").getMethodInfo();
//			CodeAttribute ca = minfo.getCodeAttribute();
//			CodeAttribute ca2 = pindo.getCodeAttribute();
//			CodeIterator i = ca.iterator();
//			CodeIterator i2 = ca2.iterator();
//			while(i.hasNext()){
////				System.out.println("ORIGINAL Processsor =>\n "+ i.get().getAttributes());
//				int index = i.next();
//				i2.next();
//			    int op = i.byteAt(index);
//			    int op2 = i2.byteAt(index);
//			    System.out.println(Mnemonic.OPCODE[op] + " -VS- "+ Mnemonic.OPCODE[op2]);
//			}
			
//			testProc.writeFile(b.outputPath);
			
//			CtClass testSrc =  b.builder.getCp().get("TestSrc");
//			b.builder.getCp().insertClassPath(new ClassClassPath(testSrc.getClass()));
//			testSrc.writeFile(b.outputPath);
			
//			baseClass.writeFile(b.outputPath);
			
		} 
		catch (CannotCompileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Packaging All classes into jar
		b.packageToJar(b.outputPath);
	}
	
}
