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


import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.api.SeepProgram;
import uk.ac.imperial.lsds.java2sdg.api.SeepProgramConfiguration;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.annotations.Partial;
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
		Schema schema = SchemaBuilder.getInstance().newField(Type.STRING, "key").newField(Type.INT, "value").build();
		DataStore netSource = new DataStore(schema, DataStoreType.NETWORK);
		
		SeepProgramConfiguration spc = new SeepProgramConfiguration();
		spc.newWorkflow("count(String key, int value)", netSource, netSource);
		
		return spc;
	}

//	public void main(){
//		String keyupdate = "testupdate"; // get data somehow
//		count(keyupdate); // call function -> implies this is an entry point
//		String keyread = "testread";
//		read(keyread);
//	}

	public void count(String key, int value){
		int newCounter = value +1;
//		if(kvstore.containsKey(key)){
//			newCounter = ((Integer)kvstore.get(key)) + 1;
//		}
		kvstore.put(key, newCounter);
	}

//	public int read(String key){
//		int counter = 0;
//		if(kvstore.containsKey(key)){
//			counter = (Integer)kvstore.get(key);
//		}
//		return counter;
//	}
}
