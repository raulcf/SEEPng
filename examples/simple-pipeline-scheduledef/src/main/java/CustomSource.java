import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

class CustomSource implements uk.ac.imperial.lsds.seep.api.operator.sources.Source {
	
    @Override
    public void setUp() {
        // TODO Auto-generated method stub  
    }
    
    @Override
    public void processData(ITuple data, API api) {
    	System.out.println("## SOURCE ##"); 
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