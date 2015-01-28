package uk.ac.imperial.lsds.seep.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	final static private Logger LOG = LoggerFactory.getLogger(Utils.class);
	
	public static String NL = System.getProperty("line.separator");
	public static String FILE_URI_SCHEME = "file://";
	
	public static String absolutePath(String resource){
		File f = new File(resource);
		return f.getAbsolutePath();
	}
	
	public static int computeIdFromIpAndPort(InetAddress ip, int port){
		return ip.hashCode() + port;
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
	
	public static String getStringRepresentationOfLocalIp(){
		String ipStr = null;
		try {
			InetAddress myIp = InetAddress.getLocalHost();
			ipStr = myIp.getHostAddress();
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ipStr;
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
	
}
