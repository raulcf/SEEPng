package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepcontrib.yarn.infrastructure.YarnClusterManager;

public class YarnAMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    final private static Logger LOG = LoggerFactory.getLogger(YarnAMRMCallbackHandler.class);
    
    private final Deque<Container> allocatedYarnContainers;
    private final YarnClusterManager inf;
    private int completedYarnContainers = 0;
    
    public YarnAMRMCallbackHandler(YarnClusterManager inf) {
        this.allocatedYarnContainers = new ArrayDeque<Container>();
        this.inf = inf;
    }
    
    @Override
    public float getProgress() {
        if (allocatedYarnContainers.isEmpty())
            return 0;
        return (float) completedYarnContainers / allocatedYarnContainers.size();
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        LOG.info("Got response from RM for container ask, allocatedCnt=" + containers.size());
        
        for (Container container : containers) {
            LOG.info("Allocated yarn container with id: {}", container.getId());
            allocatedYarnContainers.push(container);
            
            // Launch the container in a separate thread
            Thread launchThread = new Thread(new Runnable() {
                
                @Override
                public void run() {
                    try {
                        inf.startContainer(container);
                    } catch (IOException | YarnException e) {
                        LOG.error(e.getMessage());
                    }
                }
            });
            launchThread.start();
        }
    }
    
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            String containerId = status.getContainerId().toString();
            
            switch (status.getExitStatus()) {
                case 0: { 
                    LOG.info("Container " + containerId + " completed successfully");
                    ++completedYarnContainers;
                    // TODO: make job status as succeeded
                }
                case -100: {
                    LOG.info("Got an exit code of -100. This means that container " + containerId + " was killed"
                            + "by YARN, either due to being released by the application master or being 'lost'"
                            + "due to node failures etc.");
                }
                default: {
                    LOG.info("Container " + containerId + " failed with exit code " + status.getExitStatus()
                            + " " + status.getDiagnostics());
                    // TODO: implement retry logic
                }
            }
        }
        
    }

    @Override
    public void onError(Throwable error) {
        LOG.error("AMRM error: " + error.getMessage());
    }

    @Override
    public void onNodesUpdated(List<NodeReport> arg0) {
        LOG.info("AMRM Callback onNodesUpdated");
    }

    @Override
    public void onShutdownRequest() {
        LOG.info("AMRM onShutdownRequest");
    }

}
