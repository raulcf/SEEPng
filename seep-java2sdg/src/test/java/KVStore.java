/*******************************************************************************
 * Copyright (c) 2016 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Panagiotis Garefalakis
 ******************************************************************************/

import uk.ac.imperial.lsds.java2sdg.api.SeepProgram;
import uk.ac.imperial.lsds.java2sdg.api.SeepProgramConfiguration;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.annotations.Partitioned;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.state.stateimpl.SeepMap;

public class KVStore implements SeepProgram{

	@Partitioned
	public SeepMap<String, Integer> kvstore = new SeepMap<String, Integer>();
	
	@Override
	public SeepProgramConfiguration configure() {
		SeepProgramConfiguration spc = new SeepProgramConfiguration();
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.STRING, "key").newField(Type.INT, "value").build();
		DataStore netSource = new DataStore(schema, DataStoreType.NETWORK);
		spc.newWorkflow("count(String key, int value)", netSource, netSource);
		spc.newWorkflow("read(String key)", netSource, netSource);
		
		return spc;
	}

	public void count(String key, int value){
		int newCounter = value +1;
		if(kvstore.containsKey(key)){
			newCounter = ((Integer)kvstore.get(key)).intValue() + 1;
		}
		kvstore.put(key, new Integer(newCounter));
		
		/* Just a printout */
		if( (value %100000) == 0)
			System.out.println("[Count] 'Key': "+ key + " 'Value':" + kvstore.get(key)  );
	}

	public int read(String key){
		int readValue = 0;
		if(kvstore.containsKey(key)){
			readValue = ((Integer)kvstore.get(key)).intValue();
		}
		/* Just a printout */
		if( (readValue %100000) == 0)
			System.out.println("[Read] 'Key': "+ key + " 'Value':" + readValue );
		return readValue;
	}
}
