/*******************************************************************************
 * Copyright (c) 2014 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Raul Castro Fernandez - initial API and implementation
 ******************************************************************************/
package uk.ac.imperial.lsds.java2sdg.output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.TaskElement;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.OperatorBlock;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.Stream;

public class DOTExporter implements SDGExporter{

	private static DOTExporter instance = null;
	
	private DOTExporter(){
		
	}
	
	public static DOTExporter getInstance(){
		if(instance == null){
			instance = new DOTExporter();
		}
		return instance;
	}
	
	public void taskCreator(List<OperatorBlock> sdg, List<String> output){
		//task cluster
		output.add("subgraph cluster0 { \n");
		output.add("node [style=filled,color=white];\n");
		output.add("style=filled;\n");
		output.add("color=lightgrey;\n");
		
		boolean first = true;
		for(OperatorBlock ob : sdg){
			for(TaskElement te : ob.getTEs()){
					if(first){
						output.add(te.getId()+"");
						first = false;
					}else{
						output.add("->"+te.getId());
					}
					
			}
		}
		output.add(";\n");
		output.add("label = \"Task Elements\";\n");
		output.add("}\n");
		
	}
	
	public void stateCreator(List<OperatorBlock> sdg, List<String> output){
		//task cluster
		output.add("subgraph cluster1 { \n");
		output.add("node [style=filled];\n");
	//	output.add("style=filled;\n");
		output.add("color=blue;\n");
		
		boolean first = true;
		for(OperatorBlock ob : sdg){
						
			if(ob.getStateId() != -1){
				if(first){
					output.add(ob.getTE().getOpType().getStateName() +"");
					first = false;
				}else
					output.add("->"+ob.getTE().getOpType().getStateName());
			}

		}
		output.add("[color=white];\n");
		output.add("label = \"State Elements\";\n");
		output.add("}\n");
		
	}
	
	@Override
	public void export(List<OperatorBlock> sdg, String filename) {
		// first write in memory the file content
		List<String> output = new ArrayList<String>();
		output.add("digraph G {\n");
		
		//pgaref mod
		this.taskCreator(sdg, output);
		this.stateCreator(sdg, output);
		
		for(OperatorBlock ob : sdg){
			// Check stateful to paint it differently
			if(ob.getStateId() != -1){
				String stateName = ob.getTE().getOpType().getStateName();
				//output.add(""+stateName+" [shape=polygon,sides=4,peripheries=2,color=red,style=bold];\n");
				output.add(""+ stateName +"[shape=doubleoctagon,color=Gold,style=bold];\n");
				output.add(stateName+" -> "+ob.getId()+"[style=dotted];\n");
				output.add(""+ob.getId()+" [color=Turquoise,style=filled];\n");
			}
			// Check downstream to connect it appropiately
			if(ob.getDownstreamSize() > 0){
				for(Stream downstream : ob.getDownstreamOperator()){
					String me = ""+ob.getId()+"";
					String down = ""+downstream.getId()+"";
					output.add(me + " [color=Turquoise,shape=circle];\n" );
					output.add(me+" -> "+down+";\n");
				}
			}
			else{
				String me = ""+ob.getId()+"";
				String down = "sink";
				output.add(me+" -> "+down+";\n");
				output.add(down +"  [shape=Mdiamond];\n");
			}
			// Use a different shape for merge ops
			for(TaskElement te : ob.getTEs()){
				if(te.getAnn() != null && te.getAnn().equals(SDGAnnotation.COLLECTION)){
					output.add(""+ob.getId()+" [shape=doublecircle,color=black,style=bold];\n");
				}
			}
		}
		output.add("}\n");
		// then write to file
		this.writeToFile(output, filename);
	}
	
	private void writeToFile(List<String> output, String filename){
		if(!filename.endsWith(".dot")){
			filename = filename+".dot";
		}
		File f = new File(filename);
		BufferedWriter fw = null;
		try {
			fw = new BufferedWriter(new FileWriter(f));
			for(String line : output){
				fw.write(line);
			}
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			try {
				fw.flush();
				fw.close();
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}