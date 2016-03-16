package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.api.RuntimeEventTypes;
import uk.ac.imperial.lsds.seep.api.SeepChooseTask;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;

public class MDFSchedulingStrategy implements SchedulingStrategy {

	private Map<Integer, List<Object>> evaluatedResults = new HashMap<>();
	
	private int currentBestCandidate = -1;
	
	@Override
	public Stage next(ScheduleTracker tracker,
			Map<Integer, List<RuntimeEvent>> rEvents) {
		Stage head = tracker.getHead();
		Stage nextToSchedule = nextStageToSchedule(head, tracker);
		
		// We want to intercept when the next stage to schedule is CHOOSE, as this means that the explore is done
		// and that we have already chosen one (CHOOSE has executed eagerly every time an upstream finished running
		// Here we want to capture the outputs (real data output) of the chosen upstream stage and put them as input of
		// CHOOSE downstreams, then, make CHOOSE finished and go on with the scheduling process, by calling recursively
		// to this function
		if(nextToSchedule.getStageType() == StageType.CHOOSE_STAGE) {
			// Check whether we have finished the choose stage and we can go ahead
			// TODO: pick stage according to the currentBestCandidate
			
			
			// Reset CHOOSE structures to support next potential choose
			evaluatedResults = new HashMap<>();
			currentBestCandidate = -1;
		}
		
		return nextToSchedule;
	}
	
	private Stage nextStageToSchedule(Stage head, ScheduleTracker tracker) {
		Stage toReturn = null;
		if(tracker.isStageReady(head)) {
			toReturn = head;
		}
		else {
			for(Stage upstream : head.getDependencies()) {
				if(! tracker.isStageFinished(upstream)) {
					toReturn = nextStageToSchedule(upstream, tracker);
				}
			}
		}
		return toReturn;
	}
	
	@Override
	public List<Command> postCompletion(Stage finishedStage, ScheduleTracker tracker) {
		// When the recently completed stage is upstream to a Choose task, then we need to store the results it produced and
		// evaluate them (with the choose task). For that we capture here the runtime events and filter those that contain
		// results. 
		// IMPORTANT: By results, we mean the results of the EVALUATE function, and not the actual data produced by the 
		// stage that is upstream to the EVALUATE. See "picture" below
		// SOME_FUNCTION_A ---> EVALUATE ----> CHOOSE_A
		// SOME_FUNCTION_B ---> EVALUATE ----> CHOOSE_A
		// SOME_FUNCTION_C ---> EVALUATE ----> CHOOSE_A
		// That graph depicts a situation where a explore generated three possible instantiations of SOME_FUNCTION, that we
		// call A, B and C. The three results produced by those are evaluated by EVALUATE, that runs in the cluster in parallel.
		// In particular, SEEP will pipelines EVALUATE with SOME_FUNCTION. EVALUATE must be implemented by the user of the system
		// and must generate a RuntimeEvent that contains EvaluatedResults. Then, this scheduler gets those results and uses
		// them to evaluate CHOOSE. This happens below.
		//
		// Some additional detail. SOME_FUNCTION_A, B and C are scheduled sequentially in the cluster (hence each has all 
		// resources available). Whenever one of them finishes, its results are captured here. CHOOSE can be run pairwise.
		// In that way, as soon as Choose knows whether A or B is better, it can instruct to discard the other results.
		// This eager evaluation leads to a situation where C and other subsequent tasks will have more memory available.
		
		List<Command> commands = new ArrayList<>();
		Map<Integer, List<RuntimeEvent>> rEvents = tracker.getRuntimeEventsOfLastStageExecution();
		// STORE EVALUATED RESULTS FOR CURRENT STAGE
		if( ! finishedStage.getDependants().isEmpty()) {
			if(finishedStage.getDependants().iterator().next().getStageType() == StageType.CHOOSE_STAGE) {
				Stage chooseStage = finishedStage.getDependants().iterator().next();
				int seepChooseTaskId = chooseStage.getWrappedOperators().iterator().next();
				SeepChooseTask sct = (SeepChooseTask) tracker.getScheduleDescription().getOperatorWithId(seepChooseTaskId).getSeepTask();
				
				rEvents = tracker.getRuntimeEventsOfLastStageExecution();
				List<Object> evalResult = new ArrayList<>();
				for(List<RuntimeEvent> re : rEvents.values()) {
					for(RuntimeEvent r : re) {
						if(r.type() == RuntimeEventTypes.EVALUATE_RESULT.ofType()) {
							evalResult.add(r.getEvaluateResultsRuntimeEvent().getEvaluateResults());
						}
					}
				}
				evaluatedResults.put(finishedStage.getStageId(), evalResult);
				// Evaluate choose and get list of stages whose values are still useful
				this.currentBestCandidate = sct.choose(evaluatedResults);
				
				// TODO: difference between evaluatedResults and goOn are datasets to evict
				for(int stageId : evaluatedResults.keySet()) {
					if(stageId != currentBestCandidate) {
						
						// TODO: create a command to evict all these datasets that is then returned to the worker
						
					}
				}
			}
		}
		return commands;
	}

}
