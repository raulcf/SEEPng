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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soot.CompilationDeathException;
import soot.PhaseOptions;
import soot.Scene;
import soot.SootClass;
import soot.SootField;
import soot.SootMethod;
import soot.options.Options;
import soot.toolkits.graph.UnitGraph;
import soot.util.Chain;
import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.TaskElement.TaskElementBuilder;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.OperatorBlock;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.PartialSDGBuilder;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.SDGBuilder;
import uk.ac.imperial.lsds.java2sdg.codegenerator.CodeGenerator;
import uk.ac.imperial.lsds.java2sdg.flowanalysis.LiveVariableAnalysis;
import uk.ac.imperial.lsds.java2sdg.flowanalysis.TEBoundaryAnalysis;
import uk.ac.imperial.lsds.java2sdg.input.SourceCodeHandler;
import uk.ac.imperial.lsds.java2sdg.output.DOTExporter;
import uk.ac.imperial.lsds.java2sdg.output.GEXFExporter;

public class Main {

	private final static Logger LOG = LoggerFactory.getLogger(Main.class.getSimpleName());

	public static void main(String args[]) {
		/** Parse input parameters **/
		//FIXME: ra. Get rid of this options library and move to joptsimple
		//FIXME: ra. integrate *all* options as properties
		// Define options
		org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
		options.addOption("h", "help", false, "print this message");
		options.addOption("t", "target", true, "desired target. dot for DOT file OR gexf for GEXF files OR  seepjar for SEEP runnable jar");
		options.addOption("o", "output", true, "desired output file name");
		options.addOption("i", "input", true, "the name of the input program");
		options.addOption("cp", "classpath", true, "the path to additional libraries and code used by the input program");

		// generate helper
		HelpFormatter formatter = new HelpFormatter();

		// Parse arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} 
		catch (ParseException e) {
			// TODO Auto-generated catch block
			System.err.println("Parsing failed.  Reason: " + e.getMessage());
		}

		// Get values
		String className = null;
		String pathToSeepJar = "../seep-common/build/libs/seep-common-0.1.jar";
		String pathToDriverFile = null;
		String outputTarget = null;
		String outputFileName = null;
		if (cmd.hasOption("i")) {
			className = cmd.getOptionValue("i");
			if (className == null) {
				formatter.printHelp("java2sdg", options);
				System.exit(0);
			}
		} 
		else {
			formatter.printHelp("java2sdg", options);
			System.exit(0);
		}
		if (cmd.hasOption("cp")) {
			pathToDriverFile = cmd.getOptionValue("cp");
			if (pathToDriverFile == null) {
				System.err.println("cp parameter cannot be empty. Please specify the path to your classpath");
				System.exit(0);
			}
		} 
		else {
			pathToDriverFile = ".";
		}
		if (cmd.hasOption("o")) {
			outputFileName = cmd.getOptionValue("o");
			if (outputFileName == null) {
				System.err.println("o parameter cannot be empty. Please specify a name for the output file");
				System.exit(0);
			}
		} 
		else {
			formatter.printHelp("java2sdg", options);
			System.exit(0);
		}
		if (cmd.hasOption("t")) {
			outputTarget = cmd.getOptionValue("t");
			if (outputTarget == null) {
				System.err.println("t parameter cannot be empty. Please specify a target, dot/seepjar");
				System.exit(0);
			}
			if (!(outputTarget.equals("dot") || outputTarget.equals("seepjar") || outputTarget.equals("gexf"))) {
				System.err.println("Invalid target option");
				formatter.printHelp("java2sdg", options);
				System.exit(0);
			}
		} 
		else {
			outputTarget = "dot";
		}

		/** Set up SOOT for compilation and java manipulation **/

		// Get java.home to access rt.jar, required by soot
		String javaHome = System.getProperty("java.home");
		String sootClassPath = javaHome + "/lib/rt.jar:" + pathToSeepJar + ":"
				+ "../seep-worker/build/libs/seep-worker-0.1.jar" + ":./"; //FIXME: Ra. It should not need seep-worker anymore
		String pathToSourceCode = pathToDriverFile + "/" + className;

		/**
		 * Parse input program source code. This stage performs operations at source code only
		 * 
		 **/

		// Parse original source code
		LOG.info("Parsing source code...");
		SourceCodeHandler sch = SourceCodeHandler.getInstance(pathToSourceCode);
		sch.printLineAnnotation();
		LOG.info("Parsing source code...OK");

		/** Initialise soot and load input program **/

		// With the class loaded, we can then get the SootClass wrapper to work
		LOG.info("Setting soot classpath: " + sootClassPath);
		Scene.v().setSootClassPath(sootClassPath);
		Options.v().setPhaseOption("jb", "preserve-source-annotations");
		LOG.info("Loading class: " + className);

		SootClass c = null;
		try {
			System.out.println();
			c = Scene.v().loadClassAndSupport(className);
			c.setApplicationClass();
		} 
		catch (CompilationDeathException cde) {
			System.out.println();
			LOG.error(cde.getMessage());
			System.exit(1);
		}
		LOG.info("Loading class...OK");

		/** Extract fields and workflows **/

		PhaseOptions.v().setPhaseOption("tag.ln", "on"); // tell compiler to include line numbers
		// List fields and indicate which one is state
		LOG.info("Extracting state information...");
		Chain<SootField> fields = c.getFields();
		Iterator<SootField> fieldsIterator = fields.iterator();
		Map<String, InternalStateRepr> stateElements = Util.extractStateInformation(fieldsIterator);
		LOG.info("Extracting state information...OK");

		// List relevant methods (the ones that need to be analyzed)
		LOG.info("Extracting workflows...");
		Iterator<SootMethod> methodIterator = c.methodIterator();
		DriverProgramAnalyzer driverProgramAnalyzer = new DriverProgramAnalyzer();
		driverProgramAnalyzer.extractWorkflows(methodIterator, c);
		List<String> workflows = DriverProgramAnalyzer.getWorkflowNames(); //TODO: will return WorkflowRepr objects instead of strings

		LOG.info("Extracting workflows...OK");

		/** Build partialSDGs, one per workflow **/

		SDGBuilder sdgBuilder = new SDGBuilder();
		int workflowId = 0;
		// Analyse and extract a partial SDG per workflow
		for (String methodName : workflows) {
			// Build CFG
			LOG.info("Building partialSDG for workflow: " + methodName);
			UnitGraph cfg = Util.getCFGForMethod(methodName, c); // get cfg
			// Perform live variable analysis
			LiveVariableAnalysis lva = LiveVariableAnalysis.getInstance(cfg); // compute livevariables
			// Perform TE boundary analysis
			TEBoundaryAnalysis oba = TEBoundaryAnalysis.getBoundaryAnalyzer(cfg, stateElements, sch, lva);
			List<TaskElementBuilder> sequentialTEList = oba.performTEAnalysis();
			// TODO: this partialSDG will contain also a source and optionally a sink, depending on the info of the WorkflowRepr object
			List<OperatorBlock> partialSDG = PartialSDGBuilder.buildPartialSDG(sequentialTEList, workflowId);
			// for(OperatorBlock ob : partialSDG){
			// System.out.println(ob);
			// }
			workflowId++;
			sdgBuilder.addPartialSDG(partialSDG);
		}
		
		// TODO: Validate partialSDGs here.
		// TODO: we should come up with a reasonable collection of unit tests to try all type of workflows at this point
		// TODO: and observe whether they are correctly constructed or not (including sources and sinks)

		/** Build SDG from partialSDGs **/

		LOG.info("Building SDG from " + sdgBuilder.getNumberOfPartialSDGs()
				+ " partialSDGs...");
		List<OperatorBlock> sdg = sdgBuilder.synthetizeSDG();
		// for(OperatorBlock ob : sdg){
		// System.out.println(ob);
		// }
		LOG.info("Building SDG from partialSDGs...OK");

		/** Ouput SDG in a given format **/

		// Output
		if (outputTarget.equals("dot")) { // dot output
			// Export SDG to dot
			LOG.info("Exporting SDG to DOT file...");
			DOTExporter exporter = DOTExporter.getInstance();
			exporter.export(sdg, outputFileName);
			LOG.info("Exporting SDG to DOT file...OK");
		} else if (outputTarget.equals("gexf")) {
			LOG.info("Exporting GEXF to DOT file...");
			GEXFExporter exporter = GEXFExporter.getInstance();
			exporter.export(sdg, outputFileName);
			LOG.info("Exporting GEXF to DOT file...OK");
		} else if (outputTarget.equals("seepjar")) {
			LOG.info("Exporting SEEP runnable query...");
			List<OperatorBlock> assembledCode = CodeGenerator.assemble(sdg);
			for (OperatorBlock ob : assembledCode) {
				System.out.println("---->");
				System.out.println("");
				System.out.println("");
				System.out.println("");
				System.out.println("");
				System.out.println(ob.getCode());
				System.out.println("");
				System.out.println("");
				System.out.println("");
				System.out.println("");
			}
			// Set<TaskElement> sdg = SDGAssembler.getSDG(oba, lva, sch);
			//
			// // SDGAssembler sdgAssembler = new SDGAssembler();
			// // Set<OperatorBlock> sdg =
			// sdgAssembler.getFakeLinearPipelineOfStatelessOperators(1);
			//
			// QueryBuilder qBuilder = new QueryBuilder();
			// String q = qBuilder.generateQueryPlanDriver(sdg);
			// System.out.println("QueryPlan: "+q);
			// qBuilder.buildAndPackageQuery();
		}
	}
}
