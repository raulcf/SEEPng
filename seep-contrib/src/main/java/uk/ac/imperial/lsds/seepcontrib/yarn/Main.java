package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import joptsimple.OptionParser;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepcontrib.yarn.client.SeepYarnAppSubmissionClient;
import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;


public class Main {
	
	final private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private void executeMaster(String[] args, YarnConfig yc, String[] queryArgs){
		
		LOG.info("Starting Yarn Application Submission Client");
		SeepYarnAppSubmissionClient syasc = new SeepYarnAppSubmissionClient(yc);
		try {
			boolean success = syasc.submitSeepYarnApplication();
			if (!success) {
				throw new RuntimeException("Failed to submit yarn app client.");
			}
		} 
		catch (InterruptedException | YarnException | IOException e) {
			e.printStackTrace();
		}
		
		// TODO: Check how to control lifecycle of yarn client submission here...
	}
	
	public static void main(String args[]){
		// Register JVM shutdown hook
		registerShutdownHook();
		// Get Properties with command line configuration 
		List<ConfigKey> configKeys = YarnConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		// Unrecognized options are passed through to the query
		parser.allowsUnrecognizedOptions();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Get Properties with file configuration
		Properties fileProperties = Utils.readPropertiesFromFile(YarnConfig.YARN_PROPERTIES_FILE);
		
		// Merge both properties, command line has preference
		Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(commandLineProperties, fileProperties);
		boolean validates = validateProperties(validatedProperties);		
		if(!validates){
			printHelp(parser);
			System.exit(0);
		}
		YarnConfig yc = new YarnConfig(validatedProperties);

		Main instance = new Main();
		instance.executeMaster(args, yc, cla.getQueryArgs());
	}
	
	private static boolean validateProperties(Properties validatedProperties){	
		return true;
	}
	
	private static void printHelp(OptionParser parser){
		try {
			parser.printHelpOn(System.out);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void registerShutdownHook(){
		Thread hook = new Thread(new YarnAppSubmissionClientShutdownHookWorker());
		Runtime.getRuntime().addShutdownHook(hook);
	}
}