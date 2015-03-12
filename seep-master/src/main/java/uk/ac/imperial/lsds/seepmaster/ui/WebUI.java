package uk.ac.imperial.lsds.seepmaster.ui;

import java.net.URL;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;
import uk.ac.imperial.lsds.seepmaster.ui.web.WebUIHandler;

public class WebUI implements UI{

	final private static Logger LOG = LoggerFactory.getLogger(WebUI.class);
	
	private WebUIHandler actionHandler;
	
	private Server server;
	
	public WebUI(QueryManager qm, InfrastructureManager inf){
		actionHandler = new WebUIHandler(qm, inf);
		silenceJettyLogger();
		this.server = new Server(8888);
		
		// Configure resourceHandler
		ResourceHandler mainHandler = new ResourceHandler();
		mainHandler.setDirectoriesListed(true);
        mainHandler.setWelcomeFiles(new String[]{ "index.html" });
        String baseDirectory = "webui";
        URL url = this.getClass().getClassLoader().getResource(baseDirectory);
        String basePath = url.toExternalForm();
        //String path = WebUI.class.getResource("/webui").getPath();
        //mainHandler.setResourceBase(url.getPath());
        mainHandler.setResourceBase(basePath);
        
        LOG.info("Web resource base: {}", mainHandler.getBaseResource());
        
        // Configure servletHandler
        ServletHandler sHandler = new ServletHandler();
        ServletHolder sh = new ServletHolder(actionHandler);
        sHandler.addServletWithMapping(sh, "/action");
        
        // Configure all handlers
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { mainHandler, sHandler, new DefaultHandler() });
        server.setHandler(handlers);
        
        // Configure connector
        ServerConnector http = new ServerConnector(server);
        http.setHost("localhost");
        http.setPort(8080);
        http.setIdleTimeout(30000);
        server.addConnector(http);
	}
	
	@Override
	public void start() {
		try {
			server.start();
			LOG.info("Web UI running at: {}", server.getURI());
			server.join();
			
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}
	
	private void silenceJettyLogger(){
		final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("org.eclipse.jetty");
		if (!(logger instanceof ch.qos.logback.classic.Logger)) {
		    return;
		}
		ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
		logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO);
	}
}