package general;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sinks.TaggingSink;


public class Snk implements Sink, TaggingSink {

	final static Logger log = LoggerFactory.getLogger(Snk.class);

	private int count = 0;
	private int printEvery = 1000;
	
	private String dataPath = null;
	private boolean storeResults = false;
	
	public static int BUFFER_SIZE = 1;
	private List<String> buffer = new ArrayList<>();
	private List<String> snkFields = new ArrayList<String>();
	private List<Integer> idx_snkFields = new ArrayList<Integer>();

	public Snk() {
		
	}
		
	public Snk(String dataPath, boolean storeResults) {
		this.dataPath = dataPath;
		this.storeResults = storeResults;
	}
	

	@Override
	public void setUp() {
		snkFields.add("timestamp");
		snkFields.add("well");
		snkFields.add("injector");
		snkFields.add("BHP");
		snkFields.add("CHKPCT");
		snkFields.add("glRate");
		snkFields.add("flowrate");
	}

	boolean first = true;
    int idx_timestamp = 0;
    int idx_well = 0;//data.getIndexFor("well");
    int idx_injector = 0;//data.getIndexFor("injector");
    int idx_BHP = 0;//data.getIndexFor("BHP");
    int idx_CHKPCT = 0;//data.getIndexFor("CHKPCT");
    int idx_glRate = 0;//data.getIndexFor("glRate");
    int idx_flowrate = 0;//data.getIndexFor("flowrate");

	@Override
	public void processData(ITuple data, API api) {

        if(first) {
            first = false;
            idx_timestamp = data.getIndexFor("timestamp");
            idx_well = data.getIndexFor("well");
            idx_injector = data.getIndexFor("injector");
            idx_BHP = data.getIndexFor("BHP");
            idx_CHKPCT = data.getIndexFor("CHKPCT");
            idx_glRate = data.getIndexFor("glRate");
            idx_flowrate = data.getIndexFor("flowrate");
idx_snkFields.add(idx_timestamp); 
idx_snkFields.add(idx_well); 
idx_snkFields.add(idx_injector); 
idx_snkFields.add(idx_BHP); 
idx_snkFields.add(idx_CHKPCT); 
idx_snkFields.add(idx_glRate); 
idx_snkFields.add(idx_flowrate); 
        }

		if ((count++) % printEvery == 0) {
            count = 0;
			log.debug("SNK: " ); 
			log.debug("\n\nSink received {} tuples", count);
		}
		
		if (storeResults) {
			StringBuilder sb = new StringBuilder();

			//for (String key : snkFields) {
			//for (Integer idxkey : idx_snkFields) {
            
				sb.append(Long.toString(data.getLong(idx_timestamp)));
				sb.append(',');
				sb.append(Integer.toString(data.getInt(idx_well)));//.toString());
				sb.append(',');
				sb.append(Integer.toString(data.getInt(idx_injector)));//());
				sb.append(',');
				sb.append(Float.toString(data.getFloat(idx_BHP)));//.toString());
				sb.append(',');
				sb.append(Float.toString(data.getFloat(idx_CHKPCT)));//());
				sb.append(',');
				sb.append(Float.toString(data.getFloat(idx_glRate)));//.toString());
				sb.append(',');
				sb.append(Float.toString(data.getFloat(idx_flowrate)));//());
				sb.append(',');
			//}
			
			if (sb.length() >= 1)
				this.buffer.add(sb.substring(0, sb.length()-1));
			else
				this.buffer.add(sb.substring(0, sb.length()));

			if (this.buffer.size() > BUFFER_SIZE) {
				try {
					FileWriter fw = new FileWriter(this.dataPath, true);
					BufferedWriter bw = new BufferedWriter(fw);
					for (String s : this.buffer)
						bw.write(s + "\n");
					bw.close();
					fw.close();
					this.buffer.clear();
					
					log.debug("SNK: wrote results to {} ", this.dataPath);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		for (ITuple tuple : arg0)
			processData(tuple, arg1);
	}

}
