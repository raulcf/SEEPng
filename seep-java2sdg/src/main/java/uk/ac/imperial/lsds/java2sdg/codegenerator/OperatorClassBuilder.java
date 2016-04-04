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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.CtNewConstructor;
import javassist.NotFoundException;

public class OperatorClassBuilder {

	private ClassPool cp = null;


	private static Logger LOG = LoggerFactory.getLogger(OperatorClassBuilder.class.getCanonicalName());

	public OperatorClassBuilder(){
		cp = ClassPool.getDefault();
		cp.importPackage("java.util");
		cp.importPackage("uk.ac.imperial.lsds.seep.api");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.API");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.Schema");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.operator");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.operator.sinks");
	}
	
	/**
	 * Generates a new SEEP Base class
	 * Bytecode generated using Javassist 
	 * @param code
	 * @return  CtClass
	 */
	public CtClass generateBase(String code){
		CtClass cc = cp.makeClass("Base");
		CtClass[] implInterfaces = new CtClass[1];
		try{
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.QueryComposer");
			cc.setInterfaces(implInterfaces);
			
			CtMethod compose = OperatorMethodBuilder.genBaseCompose(cc, code);
			cc.addMethod(compose);
		}
		catch (NotFoundException e) {
			LOG.error("Error generating Base class {} ", e.toString());
			e.printStackTrace();
		} 
		catch (CannotCompileException e) {
			LOG.error("Error generating Base class {} ", e.toString());
			e.printStackTrace();
		}
		return cc;
	}
	
	/**
	 * Generates a new SEEP Stateless Processor class
	 * Bytecode generated using Javassist
	 * @param opName, code, schema
	 * @return  CtClass
	 */
	public CtClass generateStatelessProcessor(String opName, String schema, String [] processorFields, String classMethod, String processorCode){
		CtClass cc = cp.makeClass(opName);
		
		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.SeepTask");
			cc.setInterfaces(implInterfaces);
			
			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);
			
			//Schema field - defined globally in the Processor class
			if( schema!=null && !schema.isEmpty()){
				CtField f = CtField.make(schema, cc);
				cc.addField(f);
			}
			//Global variables
			if( processorFields != null && processorCode.length()>0){
				for(String f : processorFields){
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if(classMethod != null && !classMethod.isEmpty()){
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, classMethod);
				cc.addMethod(cMethod);
			}
			
			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, processorCode);
			LOG.info("NEW METHOD: \n" + processorCode);
			cc.addMethod(processDataSingle);
			
			//Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);
			
			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);
			
			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);
			
		}
		catch (CannotCompileException e) {
			LOG.error("Error generating Stateless Processor class {} ", e.toString());
			e.printStackTrace();
		} 
		catch (NotFoundException e) {
			LOG.error("Error generating Stateless Processor class {} ", e.toString());
			e.printStackTrace();
		}
//		cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;	
	}
	
	
	/**
	 * Generates a new SEEP Source class
	 * Bytecode generated using Javassist
	 * @param code, schema
	 * @return  CtClass
	 */
	public CtClass generateSource(String schema, String [] sourceFields, String sourceExtraMethod, String processorCode){
		CtClass cc = cp.makeClass("Src");
		
		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.operator.sources.Source");
			cc.setInterfaces(implInterfaces);
			
			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);
			
			//Schema field - defined globally in the Processor class
			if(schema!=null && !schema.isEmpty()){
				CtField f = CtField.make(schema, cc);
				cc.addField(f);
			}
			//Global variables
			if( sourceFields != null && sourceFields.length > 0 ){
				for(String f : sourceFields){
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if(sourceExtraMethod != null && !sourceExtraMethod.isEmpty()){
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, sourceExtraMethod);
				cc.addMethod(cMethod);
			}
			
			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, processorCode);
			LOG.info("NEW METHOD: \n" + processorCode);
			cc.addMethod(processDataSingle);
			
			//Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);
			
			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);
			
			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);
			
		}
		catch (CannotCompileException e) {
			LOG.error("Error generating Source class {} ", e.toString());
			e.printStackTrace();
		} 
		catch (NotFoundException e) {
			LOG.error("Error generating Source class {} ", e.toString());
			e.printStackTrace();
		}
//		cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;
	}
	
	/**
	 * Generates a new SEEP Sink class
	 * Bytecode generated using Javassist
	 * @param code
	 * @return  CtClass
	 */
	public CtClass generateSink(String schema, String [] sinkFields, String sinkExtraMethod, String processorCode){
		CtClass cc = cp.makeClass("Snk");
		
		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.operator.sinks.Sink");
			cc.setInterfaces(implInterfaces);
			
			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);
			
			//Schema field - defined globally in the Processor class
			if(schema != null && !schema.isEmpty()){
				CtField f = CtField.make(schema, cc);
				cc.addField(f);
			}
			//Global variables
			if( sinkFields != null && sinkFields.length > 0){
				for(String f : sinkFields){
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if(sinkExtraMethod != null && !sinkExtraMethod.isEmpty()){
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, sinkExtraMethod);
				cc.addMethod(cMethod);
			}
			
			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, processorCode);
			LOG.info("NEW METHOD: \n" + processorCode);
			cc.addMethod(processDataSingle);
			
			//Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);
			
			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);
			
			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);
			
		}
		catch (CannotCompileException e) {
			LOG.error("Error generating Sink class {} ", e.toString());
			e.printStackTrace();
		} 
		catch (NotFoundException e) {
			LOG.error("Error generating Sink class {} ", e.toString());
			e.printStackTrace();
		}
//		cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;
	}
	
	
	/**
	 * @return the cp
	 */
	public ClassPool getCp() {
		return cp;
	}
}
