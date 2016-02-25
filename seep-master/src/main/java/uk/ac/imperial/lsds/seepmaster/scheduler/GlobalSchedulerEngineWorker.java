package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.Kryo;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seep.scheduler.engine.ScheduleTracker;
import uk.ac.imperial.lsds.seep.scheduler.engine.SchedulingStrategy;
import uk.ac.imperial.lsds.seepmaster.query.GlobalScheduledQueryManager;

/**
 * @author pg1712@ic.ac.uk
 *
 */
public class GlobalSchedulerEngineWorker implements Runnable {

	final private Logger LOG = LoggerFactory.getLogger(GlobalSchedulerEngineWorker.class);
	
	private ScheduleDescription scheduleDescription;
	private SchedulingStrategy schedulingStrategy;
	private ScheduleTracker tracker;
	
	private Set<Connection> localSchedulerConnections;
	private  GlobalScheduledQueryManager gsm;

	private Comm comm;
	private Kryo k;
	
	private boolean work = true;
	
	/*
	 * TODO: Create a generic abstract class for SchedulerEngineWorkers in general??  LocalEngineWorker and GlobalSchedulerEngineWorker could use this abstraction
	 */
	
	public GlobalSchedulerEngineWorker(ScheduleDescription sdesc, SchedulingStrategy schedulingStrategy, Comm comm, Kryo k, GlobalScheduledQueryManager gsm) {
		this.scheduleDescription = sdesc;
		this.schedulingStrategy = schedulingStrategy;
		this.tracker = new ScheduleTracker(scheduleDescription.getStages());
		this.comm = comm;
		this.k = k;
		this.gsm =gsm;
	}

	public void stop() {
		this.work = false;
	}
	
	/**
	 * TODO: Introduce Group Tasks
	 */
	
	@Override
	public void run() {
		GlobalScheduledQueryManager se = gsm;
		LOG.info("[START GLOBAL JOB]");
		while(work) {
			// Get next stage
			Stage nextStage = schedulingStrategy.next(tracker);

			if(nextStage == null ) {
				// TODO: means the computation finished, do something
				se.__reset_schedule();
				se.__initializeEverything();
				nextStage = se.__get_next_stage_to_schedule_fot_test();
//				if(tracker.isScheduledFinished()) {
//					LOG.info("TODO: 1-Schedule has finished at this point");
//					work = false;
//					continue;
//				}
			}
			
			Set<Connection> euInvolved = this.getLocalSchedulersInvolvedInStage(nextStage);
//			trackStageCompletionAsync(nextStage, euInvolved);
			
			List<CommandToNode> commands = this.assignWorkToLocalSchedulers(nextStage, euInvolved);
			nextStage.setStageTimestamp(System.currentTimeMillis());
			LOG.info("[START] GLOBAL SCHEDULING Stage {}", nextStage.getStageId());
			for(CommandToNode ctn : commands) {
				boolean success = comm.send_object_sync(ctn.command, ctn.c, k);
			}
			tracker.setFinished(nextStage, null);
//			tracker.waitForFinishedStageAndCompleteBookeeping(nextStage);
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private class CommandToNode {
		public CommandToNode(MasterWorkerCommand command, Connection c){
			this.command = command;
			this.c = c;
		}
		public MasterWorkerCommand command;
		public Connection c;
	}
	
	private List<CommandToNode> assignWorkToLocalSchedulers(Stage nextStage, Set<Connection> conns) {
		// All input data references to process during next stage
		int nextStageId = nextStage.getStageId();
		
		List<CommandToNode> commands = new ArrayList<>();
		for(Connection c : conns) {
			MasterWorkerCommand esc = null;
			Set<Stage> toSend = new HashSet<>();
			toSend.add(nextStage);
			esc = ProtocolCommandFactory.buildLocalSchedulerStageCommand(nextStageId, toSend);
			CommandToNode ctn = new CommandToNode(esc, c);
			commands.add(ctn);
		}
		return commands;
	}
	
	private Set<Connection> getLocalSchedulersInvolvedInStage(Stage stage) {
		Set<Connection> cons = new HashSet<>();
		cons.addAll(this.localSchedulerConnections);
		return cons;
	}
	
	private void trackStageCompletionAsync(Stage stage, Set<Connection> euInvolved) {
		// Just start the tracker async
		new Thread(new Runnable() {
			public void run() {
				// Wait until stage is completed
				Set<Integer> euIds = new HashSet<>();
				for(Connection c : euInvolved) {
					euIds.add(c.getId());
				}
				tracker.trackWorkersAndBlock(stage, euIds);
			}
		}).start();
	}
	
	public boolean prepareForStart(Set<Connection> connections) {
		// Set initial connections in worker
		this.localSchedulerConnections = connections;
		// Basically change stage status so that SOURCE tasks are ready to run
		boolean success = true;
		for(Stage stage : scheduleDescription.getStages()) {
			if(stage.getStageType().equals(StageType.UNIQUE_STAGE) || stage.getStageType().equals(StageType.SOURCE_STAGE)) {
				//configureInputForInitialStage(connections, stage, slq);
				boolean changed = tracker.setReady(stage);
				success = success && changed;
			}
		}
		return success;
	}
	
	
	public void newStageStatus(int stageId, int euId, Map<Integer, Set<DataReference>> results, StageStatusCommand.Status status) {
		switch(status) {
		case OK:
			LOG.info("EU {} finishes stage {}", euId, stageId);
			tracker.finishStage(euId, stageId, results);
			break;
		case FAIL:
			LOG.info("EU {} has failed executing stage {}", euId, stageId);
			break;
		default:
			LOG.error("Unrecognized STATUS in StageStatusCommand");
		}
	}
	
	
	/** Methods to facilitate GlobalScheduler testing **/
	
	public ScheduleTracker __tracker_for_testing(){
		return tracker;
	}
	
	public Stage __next_stage_scheduler(){
		return schedulingStrategy.next(tracker);
	}
	
	public void __reset_schedule() {
		tracker.resetAllStagesTo(StageStatus.WAITING);
	}
}
