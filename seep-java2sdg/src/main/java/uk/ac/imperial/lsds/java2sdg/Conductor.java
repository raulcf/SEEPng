package uk.ac.imperial.lsds.java2sdg;

import java.util.Map;

import org.codehaus.janino.Java;

import uk.ac.imperial.lsds.java2sdg.analysis.AnnotationAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.StateAnalysis;
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
		String inputFile = cc.getString(CompilerConfig.INPUT_FILE);
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		/** Get all annotations and map them to a line **/
		Map<Integer, SDGAnnotation> annotations = AnnotationAnalysis.getAnnotations(compilationUnit);
		
		/** Extract fields **/
		Map<String, InternalStateRepr> fields = StateAnalysis.getStates(compilationUnit);
		
		/** Extract workflows **/
		Map<String, WorkflowRepr> workflows = WorkflowAnalysis.getWorkflows(compilationUnit);
		
	}
	
}
		
//		/** Extract fields and workflows **/
//
//		PhaseOptions.v().setPhaseOption("tag.ln", "on"); // tell compiler to include line numbers
//		// List fields and indicate which one is state
//		LOG.info("Extracting state information...");
//		Chain<SootField> fields = c.getFields();
//		Iterator<SootField> fieldsIterator = fields.iterator();
//		Map<String, InternalStateRepr> stateElements = Util.extractStateInformation(fieldsIterator);
//		LOG.info("Extracting state information...OK");
//
//		// List relevant methods (the ones that need to be analyzed)
//		LOG.info("Extracting workflows...");
//		Iterator<SootMethod> methodIterator = c.methodIterator();
//		DriverProgramAnalyzer driverProgramAnalyzer = new DriverProgramAnalyzer();
//		driverProgramAnalyzer.extractWorkflows(methodIterator, c);
//		List<String> workflows = DriverProgramAnalyzer.getWorkflowNames(); //TODO: will return WorkflowRepr objects instead of strings
//
//		LOG.info("Extracting workflows...OK");
//
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
