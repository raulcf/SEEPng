package uk.ac.imperial.lsds.seepmaster.ui.web;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;

public class WebUIHandler extends HttpServlet {

	final private static Logger LOG = LoggerFactory.getLogger(WebUIHandler.class);
	private static final long serialVersionUID = 1L;
	
	private final int MAX_MEMORY_SIZE_TO_HOLD_FILE = 1024 * 1024 * 100; // 100 MB
	private final int MAX_UPLOAD_SIZE_TO_HOLD_FILE = 1024 * 1024 * 100; // 100 MB
	
	private QueryManager qm;
	private InfrastructureManager inf;

	public WebUIHandler(QueryManager qm, InfrastructureManager inf){
		this.qm = qm;
		this.inf = inf;
	}
	
	@Override
	public void init(){
		
	}
	
	@Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Get parameter map
		Map<String, String[]> parameters = request.getParameterMap();
		String[] actionIdValues = parameters.get("actionid");
		int actionId = Integer.valueOf(actionIdValues[0]);
		boolean success = handleAction(actionId);
		if(!success){
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.sendRedirect("fail.html");
		}
		else{
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.sendRedirect("ok.html");
		}
    }
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		boolean success = false;
		// Check that we have a file upload request
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);
		// sanity check
		if(!isMultipart){
			success = false;
		}
		else{
			List<FileItem> items = handleFileUploadForm(request);
			success = this.handleFileItems(items);
		}
		if(!success){
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.sendRedirect("fail.html");
		}
		else{
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.sendRedirect("ok.html");
		}
	}
	
	private boolean handleFileItems(List<FileItem> items){
		String baseClass = null;
		String pathToJar = null;
		Iterator<FileItem> iter = items.iterator();
		while (iter.hasNext()) {
		    FileItem item = iter.next();

		    if (item.isFormField()) {	        
		        String name = item.getFieldName();
		        if(name.equals("baseclass")){
		        	baseClass = item.getString();
		        }
		    } 
		    else {
		        pathToJar = processUploadedFile(item);
		    }
		}
		return qm.loadQueryFromFile(pathToJar, baseClass, new String[]{});
	}
	
	private String processUploadedFile(FileItem item){
		
		File uploadedFile = new File("queryfile_viaweb.jar");
	    try {
			item.write(uploadedFile);
		} 
	    catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return uploadedFile.getAbsolutePath();
	}
	
	private List<FileItem> handleFileUploadForm(HttpServletRequest request){
		List<FileItem> items = null;
		// Create a factory for disk-based file items
		DiskFileItemFactory factory = new DiskFileItemFactory();
		factory.setSizeThreshold(MAX_MEMORY_SIZE_TO_HOLD_FILE);
		// Configure a repository (to ensure a secure temp location is used)
		ServletContext servletContext = this.getServletConfig().getServletContext();
		File repository = (File) servletContext.getAttribute("javax.servlet.context.tempdir");
		factory.setRepository(repository);

		// Create a new file upload handler
		ServletFileUpload upload = new ServletFileUpload(factory);
		upload.setSizeMax(MAX_UPLOAD_SIZE_TO_HOLD_FILE);
		// Parse the request
		try {
			items = upload.parseRequest(request);
		} 
		catch (FileUploadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return items;
	}
	
	private boolean handleAction(int action){
		boolean allowed = false;
		
		switch(action){
		case 1:
			LOG.info("Deploying query to nodes...");
			allowed = qm.deployQueryToNodes();
			if(!allowed){
				LOG.warn("Could not deploy query");
			}
			else{
				LOG.info("Deploying query to nodes...OK");
			}
			return allowed;
		case 2:
			LOG.info("Starting query...");
			allowed = qm.startQuery();
			if(!allowed){
				LOG.warn("Could not start query");
			}
			else{
				LOG.info("Starting query...OK");
			}
			return allowed;
		case 3:
			LOG.info("Stopping query...");
			allowed = qm.stopQuery();
			if(!allowed){
				LOG.warn("Could not stop query");
			}
			else{
				LOG.info("Stopping query...OK");
			}
			return allowed;
		case 4:
			LOG.info("Exit");
			return true;
		default:
				
		}
		return false;
	}

}
