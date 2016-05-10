package uk.ac.imperial.lsds.java2sdg;

import java.util.List;
import java.util.Map;

import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.java2sdg.analysis.AnnotationAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.CoarseGrainedTEAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.analysis.StateAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.TEAnalyzerStrategyType;
import uk.ac.imperial.lsds.java2sdg.analysis.WorkflowAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.WorkflowExtractorAnalysis;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGRepr;
import uk.ac.imperial.lsds.java2sdg.codegenerator.CodeGenerator;
import uk.ac.imperial.lsds.java2sdg.codegenerator.QueryBuilder;
import uk.ac.imperial.lsds.java2sdg.output.DOTExporter;
import uk.ac.imperial.lsds.java2sdg.output.OutputTarget;

public class Conductor {

	final private static Logger LOG = LoggerFactory.getLogger(Conductor.class.getCanonicalName());
	
	private CompilerConfig cc;
	private ConductorUtils cu;
	
	public Conductor(CompilerConfig cc){
		this.cc = cc;
		this.cu = new ConductorUtils();
	}
	
	public void start(){
		
		/** Get Compilation Unit **/
//		String inputFilePath = cc.getString(CompilerConfig.INPUT_FILE);
		String inputFilePath = Util.getProjectPath() +"/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFilePath);
		
		/** Extract annotations **/
		Map<Integer, SDGAnnotation> annotations = AnnotationAnalysis.getAnnotations(compilationUnit);
		
		/** Extract fields **/
		Map<String, InternalStateRepr> stateFields = StateAnalysis.getStates(compilationUnit);
		
		/** Extract workflows **/
		Map<String, CodeRepr> workflowBodies = WorkflowExtractorAnalysis.getWorkflowBody(compilationUnit);
		Map<String, WorkflowRepr> workflows = WorkflowAnalysis.getWorkflows(inputFilePath, workflowBodies);
		
		for(Map.Entry<String, WorkflowRepr> w : workflows.entrySet())
			System.out.println("Workflow: "+ w.getKey() + " V: "+ w.getValue().toString());
		
		/** Build partial SDGs from workflows **/
		// Perform live variable analysis and retrieve information
		LivenessInformation lvInfo = LVAnalysis.getLVInfo(compilationUnit);
		
		// Perform TE boundary analysis -> list of TEs
		TEAnalyzerStrategyType strategy = TEAnalyzerStrategyType.getType(cc.getInt(CompilerConfig.TE_ANALYZER_TYPE));
		
		List<PartialSDGRepr> partialSDGs = null;
		switch(strategy) {
		case COARSE:
			// coarse mode -> used mainly for debugging
			partialSDGs = CoarseGrainedTEAnalysis.getPartialSDGs(workflows, lvInfo);
			break;
		case STATE_ACCESS:
			// TODO: implement state boundaries -> atc 14
			
			break;
		}
		
		System.out.println("ALL PARTIAL SDGs: ");
		for(PartialSDGRepr p : partialSDGs)
			System.out.println(p.toString());
		
		/** Build SDG from partial SDGs **/
		SDGRepr sdg = SDGRepr.createSDGFromPartialSDG(partialSDGs);
		for (SDGNode n : sdg.getSdgNodes()) {
			System.out.println("-----------");
			System.out.println(n.toString());
			System.out.println("-----------");
		}
		
		/** Output generated SDG **/
		OutputTarget ot = OutputTarget.ofType(cc.getInt(CompilerConfig.TARGET_OUTPUT));
		String outputName = cc.getString(CompilerConfig.OUTPUT_FILE);
		switch (ot) {
		case DOT:
			LOG.info("Exporting SDG to DOT file..");
			 DOTExporter.getInstance().export(sdg, outputName);
			LOG.info("Exporting SDG to DOT file...OK");
			break;
		case GEXF:
			LOG.info("Exporting SDG to GEXF file...");

			LOG.info("Exporting SDG to GEXF file...OK");
			break;
		case X_JAR:
			LOG.info("Exporting SDG to SEEP runnable query JAR...");
			SDGRepr assembledSDG = CodeGenerator.assemble(sdg);
			QueryBuilder qBuilder = new QueryBuilder();
			qBuilder.buildAndPackageQuery(assembledSDG);
			LOG.info("Exporting SDG to SEEP runnable query JAR...OK");
			break;
		default:
			LOG.error("Unkown Output OPTION {} ", ot);

		}
		
	}	
}
