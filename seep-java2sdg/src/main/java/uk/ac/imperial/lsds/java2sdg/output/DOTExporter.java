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
import java.util.Map.Entry;

import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;

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
	
	public void taskElementCreator(SDGRepr sdg, List<String> output){
		//task cluster
		output.add("subgraph cluster0 { \n");
		output.add("node [style=filled,fillcolor=white];\n");
		output.add("style=filled;\n");
		output.add("color=lightgrey;\n");
		
		for(SDGNode node : sdg.getSdgNodes()){
			for(Entry<Integer, TaskElementRepr> te : node.getTaskElements().entrySet()){
						output.add(node.getName());
						output.add(";\n");
			}
		}
		output.add("label = \"Task Elements\";\n");
		output.add("}\n");
		
	}
	
	public void stateCreator(SDGRepr sdg, List<String> output){
		//task cluster
		output.add("subgraph cluster1 { \n");
		output.add("node [style=filled];\n");
	//	output.add("style=filled;\n");
		output.add("color=blue;\n");
		
		boolean first = true;
		for(SDGNode node : sdg.getSdgNodes()){
						
			if(node.getStateElement() != null){
				if(first){
					output.add(node.getStateElement().getStateName() +"");
					first = false;
				}else
					output.add("->"+node.getStateElement().getStateName());
			}

		}
		output.add("[color=white];\n");
		output.add("label = \"State Elements\";\n");
		output.add("}\n");
		
	}
	
	@Override
	public void export(SDGRepr sdg, String filename) {
		// first write in memory the file content
		List<String> output = new ArrayList<String>();
		output.add("digraph G {\n");
		
		//pgaref mod
		this.taskElementCreator(sdg, output);
		//TODO: NEEDS FIX
//		this.stateCreator(sdg, output);
		
		for(SDGNode node : sdg.getSdgNodes()){
			// Check stateful to paint it differently
			if(node.getStateElement() != null ){
				String stateName = node.getStateElement().getStateName();
				//output.add(""+stateName+" [shape=polygon,sides=4,peripheries=2,color=red,style=bold];\n");
				output.add(""+ stateName +"[shape=doubleoctagon,color=Gold,style=bold];\n");
				output.add(stateName+" -> "+node.getId()+"[style=dotted];\n");
				output.add(""+node.getId()+" [color=Turquoise,style=filled];\n");
			}
			if(node.isSource()){
				for(Integer downstream : node.getTaskElements().values().iterator().next().getDownstreams()){
					String me = node.getName();
					String down = sdg.getSdgNodes().get(downstream).getName();
					output.add(me +"  [shape=Mdiamond];\n");
					output.add(me+" -> "+down+";\n");
				}
			}
			// Check downstream to connect it appropiately
			else if(node.getTaskElements().values().iterator().next().getDownstreams().size() > 0){
				for(Integer downstream : node.getTaskElements().values().iterator().next().getDownstreams()){
					String me = node.getName();
					String down = sdg.getSdgNodes().get(downstream).getName();
					output.add(me + " [color=Turquoise,shape=circle];\n" );
					output.add(me+" -> "+down+";\n");
				}
			}
			else if(node.isSink()){
				String me = node.getName();
				output.add(me +"  [shape=Mcircle];\n");
			}
			// Use a different shape for merge ops
//			for(TaskElement te : ob.getTEs()){
//				if(te.getAnn() != null && te.getAnn().equals(SDGAnnotation.COLLECTION)){
//					output.add(""+ob.getId()+" [shape=doublecircle,color=black,style=bold];\n");
//				}
//			}
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