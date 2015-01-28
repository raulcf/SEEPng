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

import java.util.List;
import java.util.Properties;

import joptsimple.OptionParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class Main {

	private final static Logger LOG = LoggerFactory.getLogger(Main.class.getSimpleName());

	public static void main(String args[]) {
		/** Parse input parameters **/
		
		List<ConfigKey> configKeys = WorkerConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		CompilerConfig cc = new CompilerConfig(commandLineProperties);
		
		Conductor c = new Conductor(cc);
		c.start();
	}
}
		
//		//FIXME: ra. Get rid of this options library and move to joptsimple
//		//FIXME: ra. integrate *all* options as properties
//		// Define options
//		org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
//		options.addOption("h", "help", false, "print this message");
//		options.addOption("t", "target", true, "desired target. dot for DOT file OR gexf for GEXF files OR  seepjar for SEEP runnable jar");
//		options.addOption("o", "output", true, "desired output file name");
//		options.addOption("i", "input", true, "the name of the input program");
//		options.addOption("cp", "classpath", true, "the path to additional libraries and code used by the input program");

//		// generate helper
//		HelpFormatter formatter = new HelpFormatter();
//
//		// Parse arguments
//		CommandLineParser parser = new BasicParser();
//		CommandLine cmd = null;
//		try {
//			cmd = parser.parse(options, args);
//		} 
//		catch (ParseException e) {
//			// TODO Auto-generated catch block
//			System.err.println("Parsing failed.  Reason: " + e.getMessage());
//		}
//
//		// Get values
//		String className = null;
//		String pathToSeepJar = "../seep-common/build/libs/seep-common-0.1.jar";
//		String pathToDriverFile = null;
//		String outputTarget = null;
//		String outputFileName = null;
//		if (cmd.hasOption("i")) {
//			className = cmd.getOptionValue("i");
//			if (className == null) {
//				formatter.printHelp("java2sdg", options);
//				System.exit(0);
//			}
//		} 
//		else {
//			formatter.printHelp("java2sdg", options);
//			System.exit(0);
//		}
//		if (cmd.hasOption("cp")) {
//			pathToDriverFile = cmd.getOptionValue("cp");
//			if (pathToDriverFile == null) {
//				System.err.println("cp parameter cannot be empty. Please specify the path to your classpath");
//				System.exit(0);
//			}
//		} 
//		else {
//			pathToDriverFile = ".";
//		}
//		if (cmd.hasOption("o")) {
//			outputFileName = cmd.getOptionValue("o");
//			if (outputFileName == null) {
//				System.err.println("o parameter cannot be empty. Please specify a name for the output file");
//				System.exit(0);
//			}
//		} 
//		else {
//			formatter.printHelp("java2sdg", options);
//			System.exit(0);
//		}
//		if (cmd.hasOption("t")) {
//			outputTarget = cmd.getOptionValue("t");
//			if (outputTarget == null) {
//				System.err.println("t parameter cannot be empty. Please specify a target, dot/seepjar");
//				System.exit(0);
//			}
//			if (!(outputTarget.equals("dot") || outputTarget.equals("seepjar") || outputTarget.equals("gexf"))) {
//				System.err.println("Invalid target option");
//				formatter.printHelp("java2sdg", options);
//				System.exit(0);
//			}
//		} 
//		else {
//			outputTarget = "dot";
//		}