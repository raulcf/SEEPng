package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.config.Config;


public class SeepYarnAppSubmissionClient {
	
	final private static Logger LOG = LoggerFactory.getLogger(SeepYarnAppSubmissionClient.class);

	private YarnClient yarnClient;
	private ApplicationId appId;
	
	public SeepYarnAppSubmissionClient(Config config, Configuration conf){
		this.yarnClient = YarnClient.createYarnClient();
		this.yarnClient.init(conf);
		this.yarnClient.start();
	}
	
	public boolean submitSeepYarnApplication(){
		boolean success = false;
		
		LOG.info("Requesting new application to YARN.ResourceManager...");
		// Talk to YARN.ResourceManager to create a new application
		YarnClientApplication app = null;
		try {
			app = yarnClient.createApplication();
		} catch (YarnException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		LOG.info("Requesting new application to YARN.ResourceManager...OK");
		
		// TODO: use appResponse to sanity check that resources in the cluster are sufficient for app
		
		// Create YARN.ApplicationSubmissionContext
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appId = appContext.getApplicationId();
		LOG.info("Got YARN.applicationSubmissionContext: {}", appId.toString());
		
		// TODO: set up the submission context to create an application master, (check YARN docs)
		
		return success;
	}
}
