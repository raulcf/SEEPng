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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.CannotCompileException;
import javassist.CtClass;
import uk.ac.imperial.lsds.java2sdg.Util;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks2.TaskElement;

public class QueryBuilder {

	private String outputPath = Util.getProjectPath()+"/tmp/";
	private OperatorClassBuilder builder;
	private List<CtClass> opInstantiationCode;
	
	private static Logger LOG = LoggerFactory.getLogger(QueryBuilder.class.getCanonicalName()); 
	
	public QueryBuilder(){
		builder = new OperatorClassBuilder();
		opInstantiationCode = new ArrayList<>();
	}
	
	
	@Deprecated
	public String generateQueryPlanDriver(SDGRepr sdg){
		
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
	
	public void buildAndPackageQuery(SDGRepr sdg){
		
		for(int index=0; index < sdg.getSdgNodes().size(); index++){
			
			String operatorName = sdg.getSdgNodes().get(index).getId()+"";
			String classOperatorName = "C_"+operatorName;
			
			LOG.debug("Operator Name: {} class: {} ID: {}  ", operatorName, classOperatorName, sdg.getSdgNodes().get(index).getId());
			
			if(sdg.getSdgNodes().get(index).isSource()){
				CtClass srcInstantiation = builder.generatePeriodicSource(classOperatorName, sdg.getSdgNodes().get(index).getTaskElements().values().iterator().next().getOutputSchema());
				opInstantiationCode.add(srcInstantiation);
			}
			else if(sdg.getSdgNodes().get(index).isSink()){
				CtClass srcInstantiation = builder.generatePeriodicSink(classOperatorName, sdg.getSdgNodes().get(index - 1)
						.getTaskElements().values().iterator().next().getOutputSchema());
				opInstantiationCode.add(srcInstantiation);
			}
			//Processor stateless OR statefull
			else{
				if(sdg.getSdgNodes().get(index).getStateElement() == null){
					CtClass srcInstantiation = builder.generateSingleStatelessProcessor(classOperatorName, sdg.getSdgNodes().get(index));
					opInstantiationCode.add(srcInstantiation);
				}
			}
			
		}
		opInstantiationCode.add(builder.generateGenericBase(sdg.getSdgNodes()));

		for(CtClass c : opInstantiationCode){
			try {
				c.writeFile(this.outputPath);
			} catch (CannotCompileException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.packageToJar(this.outputPath);
		
		
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
}
