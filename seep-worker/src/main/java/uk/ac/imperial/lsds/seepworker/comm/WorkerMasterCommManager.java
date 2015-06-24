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
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.protocol.CodeCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerProtocolAPI;
import uk.ac.imperial.lsds.seep.comm.protocol.MaterializeTaskCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleStageCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StopQueryCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.util.RuntimeClassLoader;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class WorkerMasterCommManager {

	final private Logger LOG = LoggerFactory.getLogger(WorkerMasterCommManager.class.getName());
	
	private ServerSocket serverSocket;
	private Kryo k;
	private Thread listener;
	private boolean working = false;
	private RuntimeClassLoader rcl;
	
	private Conductor c;
	
	private String myIp;
	private int myPort;
	
	private String pathToQueryJar;
	private String definitionClass;
	private String[] queryArgs;
	private String methodName;
	
	public WorkerMasterCommManager(int port, WorkerConfig wc, RuntimeClassLoader rcl, Conductor c){
		this.c = c;
		this.myPort = wc.getInt(WorkerConfig.LISTENING_PORT);
		this.rcl = rcl;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol(rcl);
		try {
			serverSocket = new ServerSocket(port);
			LOG.info(" Listening on {}:{}", InetAddress.getLocalHost(), port);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		listener = new Thread(new CommMasterWorker());
		listener.setName(CommMasterWorker.class.getSimpleName());
	}
	
	public void start(){
		this.working = true;
		this.listener.start();
	}
	
	public void stop(){
		//TODO: do some other cleaning work here
		this.working = false;
	}
	
	class CommMasterWorker implements Runnable {

		@Override
		public void run() {
			while(working){
				Socket incomingSocket = null;
				PrintWriter out = null;
				try{
					// Blocking call
					incomingSocket = serverSocket.accept();
					InputStream is = incomingSocket.getInputStream();
					out = new PrintWriter(incomingSocket.getOutputStream(), true);
					Input i = new Input(is, 1000000);
					MasterWorkerCommand c = k.readObject(i, MasterWorkerCommand.class);
					short cType = c.type();
					LOG.debug("RX command with type: {}", cType);
					// CODE command
					if(cType == MasterWorkerProtocolAPI.CODE.type()){
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
					else if(cType == MasterWorkerProtocolAPI.SCHEDULE_TASKS.type()){
						LOG.info("RX Schedule_Tasks command");
						ScheduleDeployCommand sdc = c.getScheduleDeployCommand();
						out.println("ack");
						handleScheduleDeploy(sdc);
					}
					// SCHEDULE_STAGE command
					else if(cType == MasterWorkerProtocolAPI.SCHEDULE_STAGE.type()) {
						LOG.info("RX Schedule Stage command");
						ScheduleStageCommand esc = c.getScheduleStageCommand();
						out.println("ack");
						handleScheduleStage(esc);
					}
					// STARTQUERY command
					else if(cType == MasterWorkerProtocolAPI.STARTQUERY.type()){
						LOG.info("RX StartRuntime command");
						StartQueryCommand sqc = c.getStartQueryCommand();
						out.println("ack");
						handleStartQuery(sqc);
					}
					// STOPQUERY command
					else if(cType == MasterWorkerProtocolAPI.STOPQUERY.type()){
						LOG.info("RX StopRuntime command");
						StopQueryCommand sqc = c.getStopQueryCommand();
						out.println("ack");
						handleStopQuery(sqc);
					}
				}
				catch(IOException io){
					io.printStackTrace();
				}
				finally {
					if (incomingSocket != null){
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
		SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, methodName);
		// Get physical info from command
		Map<Integer, EndPoint> mapping = mtc.getMapping();
		Map<Integer, Map<Integer, Set<DataReference>>> inputs = mtc.getInputs();
		Map<Integer, Map<Integer, Set<DataReference>>> outputs = mtc.getOutputs();
 		int myOwnId = Utils.computeIdFromIpAndPort(getMyIp(), myPort);
		c.setQuery(myOwnId, slq, mapping, inputs, outputs);
		c.materializeAndConfigureTask();
	}
	
	public void handleScheduleDeploy(ScheduleDeployCommand sdc) {
		// Instantiate logical query
		SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, methodName);
		// Get schedule description
		ScheduleDescription sd = sdc.getSchedule();
		int myOwnId = Utils.computeIdFromIpAndPort(getMyIp(), myPort);
		c.configureScheduleTasks(myOwnId, sd, slq);
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
	
	private InetAddress getMyIp() {
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(myIp);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ip;
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
