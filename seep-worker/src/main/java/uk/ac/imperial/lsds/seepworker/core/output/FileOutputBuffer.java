package uk.ac.imperial.lsds.seepworker.core.output;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.core.OBuffer;

public class FileOutputBuffer implements OBuffer {

	final private static Logger LOG = LoggerFactory.getLogger(FileOutputBuffer.class);
	
	private DataReference dr;
	private int id;
	private OutputStream stream;
	
	public FileOutputBuffer(DataReference dr, int batchSize) {
		this.dr = dr;
		this.id = dr.getId();
		// Create output file attaching id for unique naming and output stream
		this.stream = createOutputFile(batchSize);
	}
	
	private BufferedOutputStream createOutputFile(int batchSize) {
		String path = dr.getDataStore().getConfig().getProperty(FileConfig.FILE_PATH);
		String pathAndFilename = path + id;
		Boolean isHDFS = new Boolean(dr.getDataStore().getConfig().getProperty(FileConfig.HDFS_SOURCE));
		if (isHDFS) {
			String hdfsUri = dr.getDataStore().getConfig().getProperty(FileConfig.HDFS_URI);
			//We have two Path types in this file, and the other is imported, so
			//fully qualify this one.
			org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(hdfsUri + pathAndFilename);
			try {
				FileSystem fs = FileSystem.get(hdfsPath.toUri(), new Configuration());
				FSDataOutputStream hdfsOutput;
				try {
					hdfsOutput = fs.create(hdfsPath, false);
				} catch (IOException ioe) {
					//file already exists, so open in append mode.
					try {
						//By convention it may be considered cleaner to check
						//if a file exists, then open or append as necessary,
						//but catching the exception like this gets rid of a
						//race condition when !exists, (created), try to open
						hdfsOutput = fs.append(hdfsPath);
					} catch (IOException e){
						// TODO Auto-generated catch block
						e.printStackTrace();
						return null;
					}
				}
				return new BufferedOutputStream(hdfsOutput);
			} catch (IOException io) {
				// TODO Auto-generated catch block
				io.printStackTrace();
				return null;
			}
		} else {
			Path p = FileSystems.getDefault().getPath(pathAndFilename);
			File outputFile = p.toFile();
			BufferedOutputStream bws = null;
			try {
				// Configured with the given batch size
				bws = new BufferedOutputStream(new FileOutputStream(outputFile), batchSize);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return bws;
		}
	}

	@Override
	public int id() {
		return id;
	}

	@Override
	public DataReference getDataReference() {
		return dr;
	}

	@Override
	public boolean drainTo(WritableByteChannel channel) {
		LOG.error("Not implemented for FileOutputBuffer");
		return false;
	}

	@Override
	public boolean write(byte[] data) {
		// write that data to the output stream
		try {			
			stream.write(data);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	@Override
	public void flush() {
		try {
			stream.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean readyToWrite() {
		LOG.error("Not implemented for FileOutputBuffer");
		return false;
	}

}
