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
package uk.ac.imperial.lsds.java2sdg;

import java.io.IOException;
import java.util.List;
import java.util.Properties;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionParser;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class Main {

	private final static Logger LOG = LoggerFactory.getLogger(Main.class.getSimpleName());

	public static void main(String args[]) {
		
		/** Parse input parameters **/
		List<ConfigKey> configKeys = CompilerConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		//TODO: pgaref Maybe get properties configuration from File as well
		
		//Validate properties by checking Input and Output are set
		if(!validateProperties(commandLineProperties)){
			printHelp(parser);
			System.exit(0);
		}
		
		CompilerConfig cc = new CompilerConfig(commandLineProperties);
		Conductor c = new Conductor(cc);
		c.start();
	}
	
	
	private static boolean validateProperties(Properties properties2Validate) {
		if(properties2Validate.get(CompilerConfig.getConfigKey(CompilerConfig.INPUT_FILE).name) == ""){
			LOG.error("Please set Compiler input.file property!");
			return false;
		}
		if(properties2Validate.get(CompilerConfig.getConfigKey(CompilerConfig.OUTPUT_FILE).name) == ""){
			LOG.error("Please set Compiler output.file property!");
			return false;
		}
		return true;
	}
	
	private static void printHelp(OptionParser JOptParser) {
		try {
			JOptParser.printHelpOn(System.out);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}