package uk.ac.imperial.lsds.seepworker.comm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.CodeCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.LocalSchedulerElectCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.LocalSchedulerStagesCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerProtocolAPI;
import uk.ac.imperial.lsds.seep.comm.protocol.MaterializeTaskCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleStageCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StopQueryCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.util.RuntimeClassLoader;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;
import uk.ac.imperial.lsds.seepworker.scheduler.LocalScheduleManager;

public class WorkerMasterCommManager {

	final private Logger LOG = LoggerFactory.getLogger(WorkerMasterCommManager.class.getName());
	
	private ServerSocket serverSocket;
	private Kryo k;
	private Thread listener;
	private boolean working = false;
	private RuntimeClassLoader rcl;
	
	private Conductor c;
	private InetAddress myIp;
	
	private int myPort;
	
	private String pathToQueryJar;
	private String definitionClass;
	private String[] queryArgs;
	private String methodName;
	
	//Local Scheduler instance
	private LocalScheduleManager lsm;
	
	public WorkerMasterCommManager(InetAddress myIp, int port, WorkerConfig wc, RuntimeClassLoader rcl, Conductor c) {
		this.c = c;
		this.myIp = myIp;
		this.myPort = wc.getInt(WorkerConfig.LISTENING_PORT);
		this.rcl = rcl;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol(rcl);
		try {
			serverSocket = new ServerSocket(port, Utils.SERVER_SOCKET_BACKLOG, myIp);
			LOG.info(" Listening on {}:{}", myIp, port);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		listener = new Thread(new CommMasterWorker());
		listener.setName(CommMasterWorker.class.getSimpleName());
	}
	
	public void start() {
		this.working = true;
		this.listener.start();
	}
	
	public void stop() {
		//TODO: do some other cleaning work here
		this.working = false;
	}
	
	class CommMasterWorker implements Runnable {

		@Override
		public void run() {
			while(working) {
				Socket incomingSocket = null;
				PrintWriter out = null;
				try {
					// Blocking call
					incomingSocket = serverSocket.accept();
					InputStream is = incomingSocket.getInputStream();
					out = new PrintWriter(incomingSocket.getOutputStream(), true);
					Input i = new Input(is, 1000000);
					MasterWorkerCommand c = k.readObject(i, MasterWorkerCommand.class);
					short cType = c.type();
					LOG.debug("RX command with type: {}", cType);
					// CODE command
					if(cType == MasterWorkerProtocolAPI.CODE.type()) {
						LOG.info("RX Code command");
						CodeCommand cc = c.getCodeCommand();
						byte[] file = cc.getData();
						LOG.info("Received query file with size: {}", file.length);
						if(cc.getDataSize() != file.length){
							// sanity check
							// TODO: throw error
						}
						// TODO: get filename from properties file
						String pathToQueryJar = "query.jar";
						File f = Utils.writeDataToFile(file, pathToQueryJar);
						out.println("ack");
						loadCodeToRuntime(f);
						// Instantiate Seep Logical Query
						handleQueryInstantiation(pathToQueryJar, cc.getBaseClassName(), cc.getQueryConfig(), cc.getMethodName());
					}
					// MATERIALIZED_TASK command
					else if(cType == MasterWorkerProtocolAPI.MATERIALIZE_TASK.type()) {
						LOG.info("RX MATERIALIZED_TASK command");
						MaterializeTaskCommand mtc = c.getMaterializeTaskCommand();
						out.println("ack");
						handleMaterializeTask(mtc);
					}
					// SCHEDULE_TASKS command
					else if(cType == MasterWorkerProtocolAPI.SCHEDULE_TASKS.type()) {
						LOG.info("RX SCHEDULE_TASKS command");
						ScheduleDeployCommand sdc = c.getScheduleDeployCommand();
						out.println("ack");
						handleScheduleDeploy(sdc);
						//set new master for proper notification => Two Level Scheduling case
						if(sdc.getStageNotificationPort() != -1){
							WorkerMasterCommManager.this.c.setMasterConn(new Connection(new EndPoint(0,
								incomingSocket.getInetAddress(), sdc.getStageNotificationPort()).extractMasterControlEndPoint()));
						}

					}
					// SCHEDULE_STAGE command
					else if(cType == MasterWorkerProtocolAPI.SCHEDULE_STAGE.type()) {
						LOG.info("RX SCHEDULE_STAGE command");
						ScheduleStageCommand esc = c.getScheduleStageCommand();
						out.println("ack");
						handleScheduleStage(esc);
					}
					/**
					 * TWO LEVEL SCHEDULING SPECIFIC 
					 */
					// LOCAL SCHEDULER ELECT command
					else if(cType == MasterWorkerProtocolAPI.LOCAL_ELECT.type()) {
						LOG.info("LOCAL SCHEDULER_ELECT command");
						LocalSchedulerElectCommand lsec = c.getLocalSchedulerElectCommand();
						out.println("ack");
						lsm = new LocalScheduleManager(lsec.getWorkerNodes(), myPort);
					}
					// LOCAL SCHEDULER STAGE command
					else if(cType == MasterWorkerProtocolAPI.LOCAL_SCHEDULE.type()){
						LOG.info("LOCAL SCHEDULER STAGE command");
						LocalSchedulerStagesCommand lssc = c.getLocalSchedulerStageCommand();
						out.println("ack");
						lsm.handleLocalStageCommand(lssc);
					}
					// LOCAL SCHEDULER GOT STAGE STATUS UPDATE
					else if(cType == MasterWorkerProtocolAPI.STAGE_STATUS.type()){
						LOG.info("RX -> LOCAL SCHEDULER STAGE Status command");
						StageStatusCommand ssc = c.getStageStatusCommand();
						LOG.debug("Local StageID {} Status {} euid {} ",ssc.getStageId(), ssc.getStatus(), ssc.getEuId());
						out.println("ack");
						//update local status tracker
						lsm.notifyStageStatus(ssc);
						//And then Notify Global scheduler
						ssc.setEuId(Utils.computeIdFromIpAndPort(myIp, myPort));
						WorkerMasterCommManager.this.c.propagateStageStatus(ssc);
					}
					/**
					 * UP TO HERE - TWO LEVEL SCHEDULING SPECIFIC 
					 */
					
					// STARTQUERY command
					else if(cType == MasterWorkerProtocolAPI.STARTQUERY.type()) {
						LOG.info("RX STARTQUERY command");
						StartQueryCommand sqc = c.getStartQueryCommand();
						out.println("ack");
						if( lsm == null)
							handleStartQuery(sqc);
						else 
							lsm.handleStartQuery();
					}
					// STOPQUERY command
					else if(cType == MasterWorkerProtocolAPI.STOPQUERY.type()) {
						LOG.info("RX STOPQUERY command");
						StopQueryCommand sqc = c.getStopQueryCommand();
						out.println("ack");
						handleStopQuery(sqc);
					}
					LOG.debug("Served command of type: {}", cType);
				}
				catch(IOException io) {
					io.printStackTrace();
				}
				finally {
					if (incomingSocket != null) {
						try {
							incomingSocket.close();
						}
						catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}		
		}	
	}	
	
	public void handleQueryInstantiation(String pathToQueryJar, String definitionClass, String[] queryArgs, String methodName) {
		this.pathToQueryJar = pathToQueryJar;
		this.definitionClass = definitionClass;
		this.queryArgs = queryArgs;
		this.methodName = methodName;
	}
	
	public void handleMaterializeTask(MaterializeTaskCommand mtc) {
		// Instantiate logical query
		LOG.info("Composing query and loading to class loader...");
		SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, methodName);
		LOG.info("Composing query and loading to class loader...OK");
		// Get physical info from command
		Map<Integer, EndPoint> mapping = mtc.getMapping();
		Map<Integer, Map<Integer, Set<DataReference>>> inputs = mtc.getInputs();
		Map<Integer, Map<Integer, Set<DataReference>>> outputs = mtc.getOutputs();
 		int myOwnId = Utils.computeIdFromIpAndPort(myIp, myPort);
 		LOG.info("Computed ID: {}", myOwnId);
		c.setQuery(myOwnId, slq, mapping, inputs, outputs);
		c.materializeAndConfigureTask();
	}
	
	public void handleScheduleDeploy(ScheduleDeployCommand sdc) {
		// Instantiate logical query
		SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, methodName);
		
		// If I am a Local Scheduler
		if (lsm != null) {
			lsm.groupScheduleDeploy(sdc, slq);
		}
		// If I am just a worker
		else {
			// Get schedule description
			ScheduleDescription sd = sdc.getSchedule();
			int myOwnId = Utils.computeIdFromIpAndPort(myIp, myPort);
			c.configureScheduleTasks(myOwnId, sd, slq);
			LOG.info("Scheduled deploy is done. Waiting for master commands...");
		}
	}

	public void handleStartQuery(StartQueryCommand sqc) {
		c.startProcessing();
	}
	
	public void handleStopQuery(StopQueryCommand sqc) {
		c.stopProcessing();
	}

	public void handleScheduleStage(ScheduleStageCommand esc) {
		int stageId = esc.getStageId();
		Map<Integer, Set<DataReference>> input = esc.getInputDataReferences();
		Map<Integer, Set<DataReference>> output = esc.getOutputDataReference();
		c.scheduleTask(stageId, input, output);
	}
	
	private void loadCodeToRuntime(File pathToCode){
		URL urlToCode = null;
		try {
			urlToCode = pathToCode.toURI().toURL();
			System.out.println("Loading into class loader: "+urlToCode.toString());
			URL[] urls = new URL[1];
			urls[0] = urlToCode;
			rcl.addURL(urlToCode);
		}
		catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
}
