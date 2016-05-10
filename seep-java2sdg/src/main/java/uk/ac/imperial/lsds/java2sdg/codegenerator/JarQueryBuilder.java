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
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.CannotCompileException;
import javassist.CtClass;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDG;
import uk.ac.imperial.lsds.java2sdg.utils.Util;

public class JarQueryBuilder {

	private String outputPath = Util.getProjectPath()+"/tmp/";
	private SeepOpClassBuilder builder;
	private List<CtClass> opInstantiationCode;
	
	private static Logger LOG = LoggerFactory.getLogger(JarQueryBuilder.class.getCanonicalName()); 
	
	public JarQueryBuilder(){
		builder = new SeepOpClassBuilder();
		opInstantiationCode = new ArrayList<>();
	}
	
	public void buildAndPackageQuery(SDG sdg){
		
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
