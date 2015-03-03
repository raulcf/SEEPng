package uk.ac.imperial.lsds.java2sdg;

import java.io.File;
import java.util.Map;

import org.codehaus.janino.Java;

import uk.ac.imperial.lsds.java2sdg.analysis.AnnotationAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.analysis.StateAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.TEAnalyzerStrategyType;
import uk.ac.imperial.lsds.java2sdg.analysis.WorkflowAnalysis;
import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;

public class Conductor {

	private CompilerConfig cc;
	private ConductorUtils cu;
	
	public Conductor(CompilerConfig cc){
		this.cc = cc;
		this.cu = new ConductorUtils();
	}
	
	public void start(){
		
		/** Get Compilation Unit **/
		String inputFilePath = cc.getString(CompilerConfig.INPUT_FILE);
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFilePath);
		
		/** Extract annotations **/
		Map<Integer, SDGAnnotation> annotations = AnnotationAnalysis.getAnnotations(compilationUnit);
		
		/** Extract fields **/
		Map<String, InternalStateRepr> fields = StateAnalysis.getStates(compilationUnit);
		
		/** Extract workflows **/
		File inputFile = cu.getFile(inputFilePath);
		String className = cu.getClassName(inputFile);
		Map<String, WorkflowRepr> workflows = WorkflowAnalysis.getWorkflows(inputFile, className);
		
		/** Build partial SDGs from workflows **/
		// Perform live variable analysis and retrieve information
		Map<String, LivenessInformation> lvInfo = LVAnalysis.getLVInfo(compilationUnit);
		
		// Perform TE boundary analysis -> list of TEs
		TEAnalyzerStrategyType strategy = TEAnalyzerStrategyType.getType(cc.getInt(CompilerConfig.TE_ANALYZER_TYPE));
		switch(strategy){
		case COARSE:
			// TODO: implement coarse mode -> debugging
			break;
		case STATE_ACCESS:
			// TODO: implement state boundaries -> atc 14
			break;
		}
		
		// TODO: assemble a partial SDG from the previous list of TEs
		
		/** Build SDG from partial SDGs **/
		// TODO: get SDG from collection of partialSDGs
		
		/** Output generated SDG **/
		// TODO: in jar by using the code generation module, in DOT or GEXF for visualization
		
	}
	
}
		
//		/** Build partialSDGs, one per workflow **/
//
//		SDGBuilder sdgBuilder = new SDGBuilder();
//		int workflowId = 0;
//		// Analyse and extract a partial SDG per workflow
//		for (String methodName : workflows) {
//			// Build CFG
//			LOG.info("Building partialSDG for workflow: " + methodName);
//			UnitGraph cfg = Util.getCFGForMethod(methodName, c); // get cfg
//			// Perform live variable analysis
//			LiveVariableAnalysis lva = LiveVariableAnalysis.getInstance(cfg); // compute livevariables
//			// Perform TE boundary analysis
//			TEBoundaryAnalysis oba = TEBoundaryAnalysis.getBoundaryAnalyzer(cfg, stateElements, sch, lva);
//			List<TaskElementBuilder> sequentialTEList = oba.performTEAnalysis();
//			// TODO: this partialSDG will contain also a source and optionally a sink, depending on the info of the WorkflowRepr object
//			List<OperatorBlock> partialSDG = PartialSDGBuilder.buildPartialSDG(sequentialTEList, workflowId);
//			// for(OperatorBlock ob : partialSDG){
//			// System.out.println(ob);
//			// }
//			workflowId++;
//			sdgBuilder.addPartialSDG(partialSDG);
//		}
//		
//		// TODO: Validate partialSDGs here.
//		// TODO: we should come up with a reasonable collection of unit tests to try all type of workflows at this point
//		// TODO: and observe whether they are correctly constructed or not (including sources and sinks)
//
//		/** Build SDG from partialSDGs **/
//
//		LOG.info("Building SDG from " + sdgBuilder.getNumberOfPartialSDGs()
//				+ " partialSDGs...");
//		List<OperatorBlock> sdg = sdgBuilder.synthetizeSDG();
//		// for(OperatorBlock ob : sdg){
//		// System.out.println(ob);
//		// }
//		LOG.info("Building SDG from partialSDGs...OK");
//
//		/** Ouput SDG in a given format **/
//
//		// Output
//		if (outputTarget.equals("dot")) { // dot output
//			// Export SDG to dot
//			LOG.info("Exporting SDG to DOT file...");
//			DOTExporter exporter = DOTExporter.getInstance();
//			exporter.export(sdg, outputFileName);
//			LOG.info("Exporting SDG to DOT file...OK");
//		} else if (outputTarget.equals("gexf")) {
//			LOG.info("Exporting GEXF to DOT file...");
//			GEXFExporter exporter = GEXFExporter.getInstance();
//			exporter.export(sdg, outputFileName);
//			LOG.info("Exporting GEXF to DOT file...OK");
//		} else if (outputTarget.equals("seepjar")) {
//			LOG.info("Exporting SEEP runnable query...");
//			List<OperatorBlock> assembledCode = CodeGenerator.assemble(sdg);
//			for (OperatorBlock ob : assembledCode) {
//				System.out.println("---->");
//				System.out.println("");
//				System.out.println("");
//				System.out.println("");
//				System.out.println("");
//				System.out.println(ob.getCode());
//				System.out.println("");
//				System.out.println("");
//				System.out.println("");
//				System.out.println("");
//			}
//			// Set<TaskElement> sdg = SDGAssembler.getSDG(oba, lva, sch);
//			//
//			// // SDGAssembler sdgAssembler = new SDGAssembler();
//			// // Set<OperatorBlock> sdg =
//			// sdgAssembler.getFakeLinearPipelineOfStatelessOperators(1);
//			//
//			// QueryBuilder qBuilder = new QueryBuilder();
//			// String q = qBuilder.generateQueryPlanDriver(sdg);
//			// System.out.println("QueryPlan: "+q);
//			// qBuilder.buildAndPackageQuery();
//		}
