package uk.ac.imperial.lsds.seepcontrib.yarn.client;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;


public class SeepYarnAppSubmissionClient {
    
    final private static Logger LOG = LoggerFactory.getLogger(SeepYarnAppSubmissionClient.class);
    
    private YarnClient yarnClient;
    private ApplicationId appId;
    private YarnConfig yc;
    private YarnConfiguration conf;
    
    public SeepYarnAppSubmissionClient(YarnConfig yc){
        this.yc = yc;
        
        if (yc.getString(YarnConfig.YARN_WORKER_PACKAGE_PATH).equals("")) {
            LOG.error("Required configuration yarn.worker.package.path missing (" +
                   "Relative path to the worker package)");
        }
        if (yc.getString(YarnConfig.YARN_APPMASTER_PACKAGE_PATH).equals("")) {
            LOG.error("Required configuration yarn.appmaster.package.path missing (" +
                    "Relative path to the app-master package)");
        }

        LOG.info("AppMaster package path: {}", yc.getString(YarnConfig.YARN_APPMASTER_PACKAGE_PATH));
        
        conf = new YarnConfiguration();
        conf.set(YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME, yc.getString(YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME));

        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(conf);
        this.yarnClient.start();
    }
    
    public boolean submitSeepYarnApplication() throws IOException, YarnException, InterruptedException {
        boolean success = false;
        
        LOG.info("Requesting new application to YARN.ResourceManager...");
        // Talk to YARN.ResourceManager to create a new application
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        LOG.info("Requesting new application to YARN.ResourceManager...OK");
        
        int appMemory = yc.getInt(YarnConfig.YARN_CONTAINER_MEMORY_MB);
        int appCores = yc.getInt(YarnConfig.YARN_CONTAINER_CPU_CORES);
        
        if (appMemory > appResponse.getMaximumResourceCapability().getMemory()) {
            throw new YarnException(String.format("You're asking for more memory (%d) than is allowed by YARN: %d",
                    appMemory, appResponse.getMaximumResourceCapability().getMemory()));
        }
        if (appCores > appResponse.getMaximumResourceCapability().getVirtualCores()) {
            throw new YarnException(String.format("You're asking for more CPU (%d) than is allowed by YARN: %d", 
                    appCores, appResponse.getMaximumResourceCapability().getVirtualCores()));
        }
        
        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
            Collections.singletonList(
                yc.getString(YarnConfig.YARN_APPMASTER_PACKAGE_PATH) +
                " --deployment_target.type 1" +
                " --ui.type 2" +
                " --master.port " + yc.getInt(YarnConfig.YARN_APPMASTER_LISTENING_PORT) +
                " --" + YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME + " " + yc.getString(YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME) +
                " --" + YarnConfig.YARN_APPMASTER_LISTENING_PORT + " " + yc.getInt(YarnConfig.YARN_APPMASTER_LISTENING_PORT) +
                " --" + YarnConfig.YARN_WORKER_PACKAGE_PATH + " " + yc.getString(YarnConfig.YARN_WORKER_PACKAGE_PATH) +
                " --" + YarnConfig.YARN_WORKER_LISTENING_PORT + " " + yc.getInt(YarnConfig.YARN_WORKER_LISTENING_PORT) +
                " --" + YarnConfig.YARN_WORKER_DATA_PORT + " " + yc.getInt(YarnConfig.YARN_WORKER_DATA_PORT) +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
            );
        
        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);
        
        // Set up resource type requirements for ApplicationMaster
        Resource capability = Resource.newInstance(appMemory, appCores);

        // Create YARN.ApplicationSubmissionContext
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appId = appContext.getApplicationId();
        LOG.info("Got YARN.applicationSubmissionContext: {}", appId.toString());
        
        appContext.setApplicationName(appId.toString());
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");
        
        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Submitting application request for: {}", appId.toString());
        if (yarnClient.submitApplication(appContext) != null)
            success = true;

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && 
               appState != YarnApplicationState.KILLED && 
               appState != YarnApplicationState.FAILED) {
          
          Thread.sleep(2000);
          appReport = yarnClient.getApplicationReport(appId);
          appState = appReport.getYarnApplicationState();
        }
        
        LOG.info("Application " + appId + " finished with state " + appState + 
                " at " + appReport.getFinishTime());

        return success;
    }
    
      private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                    c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
            Environment.CLASSPATH.name(),
            Environment.PWD.$() + File.separator + "*");
      }
    
    // TODO: not used now, integrate into yarnseepappbusmission client UI
    public ApplicationReport requestReport(){
    	ApplicationReport report = null; 
    	// Get application report for the appId we are interested in
    	try {
			report = yarnClient.getApplicationReport(appId);
		} 
    	catch (YarnException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return report;
    }

    // TODO: not used now, integrate into yarnseepappbusmission client UI
    public void kill(){
    	try {
			yarnClient.killApplication(appId);
		} 
    	catch (YarnException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
