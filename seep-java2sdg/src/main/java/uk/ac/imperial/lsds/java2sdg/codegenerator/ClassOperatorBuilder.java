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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class ClassOperatorBuilder {

	private ClassPool cp = null;
	private static Logger LOG = LoggerFactory.getLogger(ClassOperatorBuilder.class.getCanonicalName());

	public ClassOperatorBuilder(){
		cp = ClassPool.getDefault();
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.Schema");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.DataStore");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.DataStoreType");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.operator.LogicalOperator");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.QueryComposer");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.Type");
		
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.OTuple");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.ITuple");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.SeepTask");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.API");
		
		cp.importPackage("java.util.ArrayList");
		cp.importPackage("java.util.Map");
		cp.importPackage("java.util.HashMap");
	}
	
	public void includeClassPath(String path){
		try {
			cp.insertClassPath(path);
		} 
		catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public CtClass getStatefulOperatorClassTemplate(String opName){
		CtClass cc = cp.makeClass(opName);
	
		return cc;	
	}
	
	public CtClass getStatelessOperatorClass_oneTupleAtATime(String opName, String code){
		CtClass cc = cp.makeClass(opName);
		
		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.SeeTask");
			cc.setInterfaces(implInterfaces);
			
			//Fields
			CtField f = CtField.make("private static final long serialVersionUID = 1L;", cc);
			cc.addField(f);
			
			CtMethod processDataSingle = CtNewMethod.make(
	                 "public void processData(ITuple data, API api) {\n"+code+"\n }", cc);
			LOG.info("NEW METHOD: ");
			LOG.info(processDataSingle.toString());
			cc.addMethod(processDataSingle);
			
			//Mandatory methods
			CtMethod setUp = CtNewMethod.make(
					"public void setUp() { }", cc);
			cc.addMethod(setUp);
			
			CtMethod close = CtNewMethod.make(
					"public void close() { }", cc);
			cc.addMethod(close);
			
			CtMethod processDataGroup = CtMethod.make(
					"public void processDataGroup(List<ITuple> arg0, API arg1) { }", cc);
			cc.addMethod(processDataGroup);
			
		}
		catch (CannotCompileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return cc;	
	}
	
//	public CtClass getStatelessOperatorClass_window(String opName, String code){
//		CtClass cc = cp.makeClass(opName);
//		
//		CtClass[] implInterfaces = new CtClass[1];
//		try {
//			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.operator.StatelessOperator"); 
//			//cc.setSuperclass(cp.get("uk.ac.imperial.lsds.seep.operator.Operator"));
//			cc.setInterfaces(implInterfaces);
//			
//			//Fields
//			CtField f = CtField.make("private static final long serialVersionUID = 1L;", cc);
//			cc.addField(f);
//			
//			//Mandatory methods
//			CtMethod processDataSingle = CtNewMethod.make(
//	                 "public void processData(DataTuple data) { }", cc);
//			cc.addMethod(processDataSingle);
//			
//			CtMethod processDataBatch = CtNewMethod.make(
//	                 "public void processData(ArrayList data) {\n"+code+"\n }", cc);
//			cc.addMethod(processDataBatch);
//			
//			CtMethod setUp = CtNewMethod.make(
//	                 "public void setUp() { }", cc);
//			cc.addMethod(setUp);
//			
//		}
//		catch (CannotCompileException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
//		catch (NotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		cp.insertClassPath(new ClassClassPath(cc.getClass()));
//		return cc;	
//	}
	
	
	public CtClass getBaseIClass(String code){
		CtClass cc = cp.makeClass("Base");
		CtClass[] implInterfaces = new CtClass[1];
		try{
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.QueryComposer");
			cc.setInterfaces(implInterfaces);
			
			CtMethod compose = CtNewMethod.make(
					"public uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery compose() {" +
	                 code +
	                 "}", cc);
			cc.addMethod(compose);
		}
		catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (CannotCompileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return cc;
	}
	
}
