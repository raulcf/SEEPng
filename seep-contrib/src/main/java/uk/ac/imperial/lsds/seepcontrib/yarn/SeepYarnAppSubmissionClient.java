package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;


public class SeepYarnAppSubmissionClient {
    
    final private static Logger LOG = LoggerFactory.getLogger(SeepYarnAppSubmissionClient.class);
    final private static String applicationType = "SEEP";
    
    private YarnClient yarnClient;
    private ApplicationId appId;
    private Configuration conf;
    
    public SeepYarnAppSubmissionClient(Config config, Configuration conf){
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(conf);
        this.yarnClient.start();
        this.conf = conf;
    }
    
    public boolean submitSeepYarnApplication() throws IOException, YarnException {
        boolean success = false;
        
        LOG.info("Requesting new application to YARN.ResourceManager...");
        // Talk to YARN.ResourceManager to create a new application
        YarnClientApplication app = null;
        app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        LOG.info("Requesting new application to YARN.ResourceManager...OK");
        
        // TODO: get the actual configuration from master
        int maxMemory = Integer.parseInt(conf.get(YarnConfig.YARN_CONTAINER_MEMORY_MB));
        int maxCores = Integer.parseInt(conf.get(YarnConfig.YARN_CONTAINER_CPU_CORES));
        
        if (maxMemory > appResponse.getMaximumResourceCapability().getMemory()) {
            throw new YarnException(String.format("You're asking for more memory (%d) than is allowed by YARN: %d",
                    maxMemory, appResponse.getMaximumResourceCapability().getMemory()));
        }
        if (maxCores > appResponse.getMaximumResourceCapability().getVirtualCores()) {
            throw new YarnException(String.format("You're asking for more CPU (%d) than is allowed by YARN: %d", 
                    maxCores, appResponse.getMaximumResourceCapability().getVirtualCores()));
        }
        
        // Create YARN.ApplicationSubmissionContext
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appId = appContext.getApplicationId();
        LOG.info("Got YARN.applicationSubmissionContext: {}", appId.toString());
        
        // TODO: set up the submission context to create an application master, (check YARN docs)
        appContext.setApplicationName(appId.toString());
        
        Resource resource = Records.newRecord(Resource.class);
        ContainerLaunchContext containerCtx = Records.newRecord(ContainerLaunchContext.class);
        LocalResource packageResource = Records.newRecord(LocalResource.class);
        
        // set the local package so that the containers and app master are provisioned with it
        Path packagePath = new Path(conf.get(YarnConfig.YARN_WORKER_PACKAGE_PATH));
        URL packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath);
        FileStatus fileStatus = packagePath.getFileSystem(conf).getFileStatus(packagePath);
        
        // TODO: check if this is all we need to configure the package
        packageResource.setResource(packageUrl);
        packageResource.setSize(fileStatus.getLen());
        packageResource.setTimestamp(fileStatus.getModificationTime());
        packageResource.setType(LocalResourceType.ARCHIVE);
        packageResource.setVisibility(LocalResourceVisibility.APPLICATION);
        
        resource.setMemory(maxMemory);
        resource.setVirtualCores(maxCores);
        appContext.setResource(resource);
        
        //TODO: set commands to run on application
        appContext.setAMContainerSpec(containerCtx);
        appContext.setApplicationType(applicationType);
        LOG.info("Submitting application request for: {}", appId.toString());
        
        if(yarnClient.submitApplication(appContext) != null)
            success = true;
        
        return success;
    }
}
