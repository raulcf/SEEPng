package uk.ac.imperial.lsds.seep.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class Utils {

	final static private Logger LOG = LoggerFactory.getLogger(Utils.class);
	
	public static int SERVER_SOCKET_BACKLOG = 10;
	public static String NL = System.getProperty("line.separator");
	public static String FILE_URI_SCHEME = "file://";
	
	public static String absolutePath(String resource){
		File f = new File(resource);
		return f.getAbsolutePath();
	}
	
	public static int computeIdFromIpAndPort(InetAddress ip, int port){
		int hash = 23;
		hash = hash * 31 + ip.hashCode();
		hash = hash * 31 + port;
		return hash;
	}
	
	public static int computeIdFromIpAndPort(String ip, int port) {
		InetAddress ip2 = null;
		try {
			ip2 = InetAddress.getByName(ip);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ip2.hashCode() + port;
	}
	
	public static File writeDataToFile(byte[] serializedFile, String fileName){
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(new File(fileName));
			fos.write(serializedFile);
			fos.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		//At this point we should have the file on disk
		File pathToCode = new File(fileName);
		if(!pathToCode.exists()){
			return null;
		}
		return pathToCode;
	}
	
	public static byte[] readDataFromFile(String path){
		FileInputStream fis = null;
		long fileSize = 0;
		byte[] data = null;
		try {
			//Open stream to file
			LOG.debug("Opening stream to file: {}", path);
			File f = new File(path);
			fis = new FileInputStream(f);
			fileSize = f.length();
			//Read file data
			data = new byte[(int)fileSize];
			int readBytesFromFile = fis.read(data);
			//Check if we have read correctly
			if(readBytesFromFile != fileSize){
				LOG.warn("Mismatch between read bytes and file size");
				fis.close();
				return null;
			}
			//Close the stream
			fis.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			try {
				fis.close();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		return data;
	}
	
	public static Properties readPropertiesFromFile(String fileName, String resFileName){
		Properties prop = new Properties();
		File f = new File(fileName);
		try{
			InputStream fis = null;
			if(f.exists()){
				// Read from file
				fis = new FileInputStream(new File(fileName));
			}
			else{
				// Read from resource
				fis = (InputStream) Thread.currentThread().getContextClassLoader().getResourceAsStream(resFileName);
			}
			if(fis != null)
				prop.load(fis);
		}
		catch(FileNotFoundException fnfe){
			LOG.error("File {} not found while trying to read properties", fileName);
			fnfe.printStackTrace();
		} 
		catch(IOException io){
			LOG.error("IOException while reading properties", fileName);
			io.printStackTrace();
		}
		return prop;
	}
	
	public static Properties overwriteSecondPropertiesWithFirst(Properties commandLineProperties, Properties fileProperties) {
		for(Object key : commandLineProperties.keySet()){
			fileProperties.put(key, commandLineProperties.get(key));
		}
		return fileProperties;
	}
	
	public static InetAddress getIpFromStringRepresentation(String ipStr) {
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(ipStr);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ip;
	}
	
	public static String getStringRepresentationOfIp(InetAddress ip){
		return ip.getHostAddress();
	}
	
	public static InetAddress getLocalIp(){
		InetAddress myIp = null;
		try {
			myIp = InetAddress.getLocalHost();
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return myIp;
	}
	
	public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }
	
	public static SeepLogicalQuery executeComposeFromQuery(String pathToJar, String definitionClass, String[] queryArgs, String methodName) {
		Class<?> baseI = null;
		Object baseInstance = null;
		Method compose = null;
		SeepLogicalQuery lsq = null;
		File urlPathToQueryDefinition = new File(pathToJar);
		LOG.debug("-> Set path to query definition: {}", urlPathToQueryDefinition.getAbsolutePath());
		URL[] urls = new URL[1];
		try {
			urls[0] = urlPathToQueryDefinition.toURI().toURL();
		}
		catch (MalformedURLException e) {
			e.printStackTrace();
		}
		// First time it is created we pass the urls
		URLClassLoader ucl = new URLClassLoader(urls);
		try {
			baseI = ucl.loadClass(definitionClass);
			// For backwards compatibility, use the default constructor if one with a string array argument is not found
			try {
				baseInstance = baseI.getConstructor(String[].class).newInstance((Object)queryArgs);
			} catch (NoSuchMethodException e) {
				baseInstance = baseI.newInstance();
				if (queryArgs.length > 0) {
					LOG.warn("Query arguments specified but Base class has no constructor taking a String[] argument");
				}
			}
			compose = baseI.getDeclaredMethod(methodName, (Class<?>[])null);
			lsq = (SeepLogicalQuery) compose.invoke(baseInstance, (Object[])null);
			ucl.close();
		}
		catch (SecurityException e) {
			e.printStackTrace();
		} 
		catch (NoSuchMethodException e) {
			e.printStackTrace();
		} 
		catch (IllegalArgumentException e) {
			e.printStackTrace();
		} 
		catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		catch (InvocationTargetException e) {
			e.printStackTrace();
		} 
		catch (InstantiationException e) {
			e.printStackTrace();
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		//Finally we return the queryPlan
		return lsq;
	}
	
	public static <K,V> String printMap(Map<K, V> map) {
		StringBuffer sb = new StringBuffer();
		for(K k : map.keySet()) {
			sb.append("K: "+k.toString()+" V: "+map.get(k).toString());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}
	
}
