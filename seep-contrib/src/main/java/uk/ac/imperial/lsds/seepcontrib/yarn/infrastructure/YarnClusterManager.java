package uk.ac.imperial.lsds.seepcontrib.yarn.infrastructure;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnit;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;
import uk.ac.imperial.lsds.seep.infrastructure.InfrastructureManager;
import uk.ac.imperial.lsds.seepcontrib.yarn.YarnAMRMCallbackHandler;
import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;

public class YarnClusterManager implements InfrastructureManager {
    
    final private static Logger LOG = LoggerFactory.getLogger(YarnClusterManager.class);
    
    public final ExecutionUnitType executionUnitType = ExecutionUnitType.YARN_CONTAINER;
    private AMRMClientAsync<ContainerRequest> amClient;
    private NMClient containerManager;
    private int containerMemory;
    private int containerCores;
    private Deque<ExecutionUnit> executionUnits;
    private Map<Integer, Connection> connectionsToPhysicalNodes;
    private YarnConfig yc;
    
    public YarnClusterManager() {
        this.executionUnits = new ArrayDeque<ExecutionUnit>();
        this.connectionsToPhysicalNodes = new HashMap<>();
    }
    
    public void init(Config yc) {
        this.yc = (YarnConfig) yc;
        
        YarnConfiguration config = new YarnConfiguration();
        config.set(YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME, yc.getString(YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME));
        
        LOG.info("Worker package path: {}", yc.getString(YarnConfig.YARN_WORKER_PACKAGE_PATH));
        
        Map<String, String> envs = System.getenv();
        String containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }
        LOG.info("Starting AppMaster Client...");

        YarnAMRMCallbackHandler amHandler = new YarnAMRMCallbackHandler(this);
        
        // TODO: get heart-beet interval from config instead of 100 default value
        amClient = AMRMClientAsync.createAMRMClientAsync(1000, amHandler);
        amClient.init(config);
        amClient.start();
           
        LOG.info("Starting AppMaster Client OK");
        
        containerManager = NMClient.createNMClient();
        containerManager.init(config);
        containerManager.start();
        
        String appMasterHostname = NetUtils.getHostname();
        // TODO Might want to define custom tracking URL
        String appMasterTrackingUrl = "";
        
        // Register self with ResourceManager. This will start heart-beating to the RM
        RegisterApplicationMasterResponse response = null;
        
        LOG.info("Register AppMaster on: " + appMasterHostname + "...");
        
        try {
            response = amClient.registerApplicationMaster(appMasterHostname, 0, appMasterTrackingUrl);
        } catch (YarnException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        
        LOG.info("Register AppMaster OK");
        
        // Dump out information about cluster capability as seen by the resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
        
        containerMemory = yc.getInt(YarnConfig.YARN_CONTAINER_MEMORY_MB);
        containerCores = yc.getInt(YarnConfig.YARN_CONTAINER_CPU_CORES);
        
        // A resource ask cannot exceed the max.
        if (containerMemory > maxMem) {
          LOG.info("Container memory specified above max threshold of cluster."
              + " Using max value." + ", specified=" + containerMemory + ", max="
              + maxMem);
          containerMemory = maxMem;
        }
    
        if (containerCores > maxVCores) {
          LOG.info("Container virtual cores specified above max threshold of  cluster."
            + " Using max value." + ", specified=" + containerCores + ", max=" + maxVCores);
          containerCores = maxVCores;
        }
        List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
        LOG.info("Received " + previousAMRunningContainers.size()
                + " previous AM's running containers on AM registration.");
    }
    
    public void requestContainer() {
        Resource capability = Records.newRecord(Resource.class);
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(containerCores);
        
        AMRMClient.ContainerRequest request =
                new AMRMClient.ContainerRequest(capability, null, null, priority);
        amClient.addContainerRequest(request);
        
        LOG.info("Submit container request: " + request.toString());
        LOG.info("Available resources: " + amClient.getAvailableResources().toString());
    }
    
    public void startContainer(Container container) throws IOException, YarnException {
        long containerId = container.getId().getContainerId();
        
        // Set up the container launch context for seep-worker
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setCommands(
            Collections.singletonList(
                yc.getString(YarnConfig.YARN_WORKER_PACKAGE_PATH) +
                " --master.ip " + NetUtils.getHostname().split("/")[1] +
                " --master.port " + String.valueOf(yc.getInt(YarnConfig.YARN_APPMASTER_LISTENING_PORT)) +
                " --worker.port " + String.valueOf(yc.getInt(YarnConfig.YARN_WORKER_LISTENING_PORT) + containerId) +
                " --data.port " +  String.valueOf(yc.getInt(YarnConfig.YARN_WORKER_DATA_PORT) + containerId) +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
            );
        context.setEnvironment(System.getenv());
        LOG.info("Starting yarn container with id: {}", containerId);
        containerManager.startContainer(container, context);
    }
    
	@Override
	public ExecutionUnit buildExecutionUnit(InetAddress ip, int port, int dataPort) {
	    return new YarnContainer(ip, port, dataPort);
	}

	@Override
	public void addExecutionUnit(ExecutionUnit eu) {
		executionUnits.push(eu);
		connectionsToPhysicalNodes.put(eu.getId(), new Connection(eu.getEndPoint()));
	}
	
	@Override
	public ExecutionUnit getExecutionUnit() {
	    if(!executionUnits.isEmpty()) {
            LOG.debug("Returning 1 executionUnit, remaining: {}", executionUnits.size()-1);
            ExecutionUnit eu = executionUnits.pop();
            return eu;
        }
        else{
            LOG.error("No available executionUnits !!!");
            return null;
        }
	}

	@Override
	public boolean removeExecutionUnit(int id) {
	    for(ExecutionUnit eu : executionUnits){
            if(eu.getId() == id){
                boolean success = executionUnits.remove(eu);
                if(success){
                    LOG.info("ExecutionUnit id: {} was removed");
                    return true;
                }
            }
        }
        return false;
	}

	@Override
	public int executionUnitsAvailable() {
		return executionUnits.size();
	}

	@Override
	public void claimExecutionUnits(int numExecutionUnits) {
		// TODO Auto-generated method stub
	}

	@Override
	public void decommisionExecutionUnits(int numExecutionUnits) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void decommisionExecutionUnit(ExecutionUnit node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Connection> getConnectionsTo(Set<Integer> executionUnitIds) {
	    Set<Connection> cs = new HashSet<>();
        for(Integer id : executionUnitIds) {
            // TODO: check that the conn actually exists
            cs.add(connectionsToPhysicalNodes.get(id));
        }
        return cs;
	}

	@Override
	public Connection getConnectionTo(int executionUnitId) {
	    return connectionsToPhysicalNodes.get(executionUnitId);
	}
	
	public void stop() {
	    FinalApplicationStatus status = FinalApplicationStatus.SUCCEEDED;
	    
	    try {
	        amClient.unregisterApplicationMaster(status, "Finished", null);
	    } catch (YarnException ex) {
	        LOG.error("Failed to unregister application", ex);
	    } catch (IOException e) {
	        LOG.error("Failed to unregister application", e);
	    }
	      
	    amClient.stop();
	    containerManager.stop();
	}
}
