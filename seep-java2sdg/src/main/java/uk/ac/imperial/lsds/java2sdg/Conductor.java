package uk.ac.imperial.lsds.java2sdg;

import java.util.List;
import java.util.Map;

import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.java2sdg.analysis.AnnotationAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LiveVariableAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LiveVariableAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.analysis.strategies.CoarseGrainedTEAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.strategies.TEAnalyzerStrategyType;
import uk.ac.imperial.lsds.java2sdg.analysis.StateAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.WorkflowConfigurationAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.WorkflowTraverserAnalysis;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDG;
import uk.ac.imperial.lsds.java2sdg.codegenerator.CodeGenerator;
import uk.ac.imperial.lsds.java2sdg.codegenerator.JarQueryBuilder;
import uk.ac.imperial.lsds.java2sdg.config.CompilerConfig;
import uk.ac.imperial.lsds.java2sdg.output.DOTExporter;
import uk.ac.imperial.lsds.java2sdg.output.OutputTargetTypes;
import uk.ac.imperial.lsds.java2sdg.utils.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.utils.Util;

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
		String inputFilePath = Util.getProjectPath() +"/src/test/java/KVStore.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFilePath);
		
		/** Extract annotations **/
		Map<Integer, SDGAnnotation> annotations = AnnotationAnalysis.getAnnotations(compilationUnit);
		/** Extract fields **/
		Map<String, InternalStateRepr> stateFields = StateAnalysis.getStates(compilationUnit);
		
		/** Extract workflows **/
		Map<String, CodeRepr> workflowBodies = WorkflowTraverserAnalysis.getWorkflowBody(compilationUnit);
		Map<String, WorkflowRepr> workflows = WorkflowConfigurationAnalysis.getWorkflows(inputFilePath, workflowBodies);
		
		for(Map.Entry<String, WorkflowRepr> w : workflows.entrySet())
			System.out.println("Workflow: "+ w.getKey() + " V: "+ w.getValue().toString());
		
		/** Build partial SDGs from workflows **/
		// Perform live variable analysis and retrieve information
		LivenessInformation lvInfo = LiveVariableAnalysis.getLVInfo(compilationUnit);
		
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
		SDG sdg = SDG.createSDGFromPartialSDG(partialSDGs, stateFields);
		for (SDGNode n : sdg.getSdgNodes()) {
			System.out.println("-----------");
			System.out.println(n.toString());
			System.out.println("-----------");
		}
		
		/** Output generated SDG **/
		OutputTargetTypes ot = OutputTargetTypes.ofType(cc.getInt(CompilerConfig.TARGET_OUTPUT));
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
			SDG assembledSDG = CodeGenerator.assemble(sdg);
			JarQueryBuilder qBuilder = new JarQueryBuilder();
			qBuilder.buildAndPackageQuery(assembledSDG);
			LOG.info("Exporting SDG to SEEP runnable query JAR...OK");
			break;
		default:
			LOG.error("Unkown Output OPTION {} ", ot);

		}
		
	}	
}
