import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

class CustomSink implements uk.ac.imperial.lsds.seep.api.operator.sinks.Sink {
	
        @Override
        public void setUp() {
            // TODO Auto-generated method stub  
        }
        
        @Override
        public void processData(ITuple data, API api) {
        	System.out.println("## SINK ##");
        }
        
        @Override
        public void processDataGroup(List<ITuple> dataList, API api) {
            // TODO Auto-generated method stub  
        }
        
        @Override
        public void close() {
            // TODO Auto-generated method stub  
        }
    }