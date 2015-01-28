/*******************************************************************************
 * Copyright (c) 2014 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Panagiotis Garefalakis - GEXF implementation
 ******************************************************************************/
package uk.ac.imperial.lsds.java2sdg.output;

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.viz.NodeShape;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.TaskElement;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.OperatorBlock;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.Stream;

public class GEXFExporter implements SDGExporter{

	//singleton instance
	private static volatile GEXFExporter instance = null; 
	
	private GEXFExporter() {
		//If called the instance should be always null!!!
		if(instance != null)
			throw new IllegalStateException("GEXFExporter already instantiated!!");
	}
	
	
	public static GEXFExporter getInstance(){
		
		if(instance == null){
			synchronized(GEXFExporter.class){
				if(instance == null)
					instance = new GEXFExporter();
			}
		}
		return instance;
	}
	
	
	public void  export(List<OperatorBlock> sdg, String filename){
		Gexf gexf = new GexfImpl();
		Calendar date = Calendar.getInstance();
		
		gexf.getMetadata()
			.setLastModified(date.getTime())
			.setCreator("Seep project Imperial College London")
			.setDescription("Java2SDG Graph");
		gexf.setVisualization(true);

		Graph graph = gexf.getGraph();
		graph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.STATIC);
		
		
		/*
		 * Initializing Node Type
		 * And Attributed going to be used
		 */
		
		AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(attrList);
		
		/*
		 * Possible Attributes, can be expanded!
		 */
		Attribute stateAttr = attrList.createAttribute("0", AttributeType.STRING, "state");
		Attribute taskAttr = attrList.createAttribute("1", AttributeType.STRING, "task");
		Attribute idAttr = attrList.createAttribute("2", AttributeType.STRING, "id");
		Attribute workflowIdAttr = attrList.createAttribute("3", AttributeType.STRING, "workflowID");
		Attribute globalAttr = attrList.createAttribute("4", AttributeType.BOOLEAN, "global")
				.setDefaultValue("true");
	 
		Map<Integer, Node> nodeTable  = new HashMap<Integer,Node>();
		//Parsing the operations	
		for(OperatorBlock ob : sdg){
			// Check stateful to paint it differently
			if(ob.getStateId() != -1){
				String stateName = ob.getTE().getOpType().getStateName();
				/*	output.add(""+stateName+" [shape=triangle,color=red,style=bold];\n");
					output.add(stateName+" -> "+ob.getId()+";\n");
					output.add(""+ob.getId()+" [color=green,style=filled];\n");
				*/
				
				Node tmp1 = graph.createNode(stateName);
				tmp1.setLabel(stateName)
					.setSize(20)
					.getAttributeValues()
						.addValue(stateAttr, "1")
						.addValue(taskAttr, ob.getTE().getOpName())
						.addValue(idAttr, ob.getTE().getAnn().name())
						.addValue(workflowIdAttr, stateName)
						.addValue(globalAttr, false+"");
				tmp1.getShapeEntity().setNodeShape(NodeShape.DIAMOND);
				
				Node tmp2 = graph.createNode(ob.getId()+"");
				tmp2.setLabel(ob.getId()+"")
					.setSize(20)
					.getAttributeValues()
						.addValue(stateAttr, "0")
						.addValue(taskAttr, ob.getTE().getOpName())
						.addValue(idAttr, ob.getId()+"")
						.addValue(workflowIdAttr, ob.getWorkflowId()+"")
						.addValue(globalAttr, false+"");
				tmp2.getShapeEntity().setNodeShape(NodeShape.SQUARE);
				
				nodeTable.put(ob.getId(), tmp1);
				nodeTable.put(ob.getId(), tmp2);
				
				tmp1.connectTo(tmp2);
				
			}
			// Check downstream to connect it appropiately
			if(ob.getDownstreamSize() > 0){
				for(Stream downstream : ob.getDownstreamOperator()){
					/*	String me = ""+ob.getId()+"";
						String down = ""+downstream.getId()+"";
						output.add(me+" -> "+down+";\n");
					*/
					
					Node tmp1 = graph.createNode(ob.getId()+"");
					tmp1.setLabel(ob.getId()+"")
						.setSize(20)
						.getAttributeValues()
							.addValue(stateAttr, "0")
							.addValue(taskAttr, ob.getTE().getOpName())
							.addValue(idAttr, ob.getId()+"")
							.addValue(workflowIdAttr, ob.getWorkflowId()+"")
							.addValue(globalAttr, false+"");
					tmp1.getShapeEntity().setNodeShape(NodeShape.SQUARE);
					
					Node tmp2 = graph.createNode(downstream.getId()+"");
					tmp2.setLabel(downstream.getId()+"")
						.setSize(20)
						.getAttributeValues()
							.addValue(stateAttr, "0")
							.addValue(taskAttr, "nop")
							.addValue(idAttr, downstream.getId()+"")
							.addValue(workflowIdAttr, downstream.getWorkflowId()+"")
							.addValue(globalAttr, false+"");
					tmp2.getShapeEntity().setNodeShape(NodeShape.SQUARE);
					
					nodeTable.put(ob.getId(), tmp1);
					nodeTable.put(ob.getId(), tmp2);
					
					tmp1.connectTo(tmp2);
					
					
					
				}
			}
			else{
				/*	String me = ""+ob.getId()+"";
					String down = "sink";
					output.add(me+" -> "+down+";\n");
				*/
				
				Node tmp1 = graph.createNode(ob.getId()+"");
				tmp1.setLabel(ob.getId()+"")
					.setSize(20)
					.getAttributeValues()
						.addValue(stateAttr, "0")
						.addValue(taskAttr, ob.getTE().getOpName())
						.addValue(idAttr,ob.getId()+"")
						.addValue(workflowIdAttr, ob.getWorkflowId()+"")
						.addValue(globalAttr, false+"");
				tmp1.getShapeEntity().setNodeShape(NodeShape.SQUARE);
				
				Node tmp2 = graph.createNode("sink");
				tmp2.setLabel("sink")
					.setSize(20)
					.getAttributeValues()
						.addValue(stateAttr, "0")
						.addValue(taskAttr, "nop")
						.addValue(idAttr, ob.getId()+"")
						.addValue(workflowIdAttr, ob.getWorkflowId()+"")
						.addValue(globalAttr, false+"");
				tmp2.getShapeEntity().setNodeShape(NodeShape.DISC);
				
				nodeTable.put(ob.getId(), tmp1);
				nodeTable.put(ob.getId(), tmp2);
				
				tmp1.connectTo(tmp2);
			}
			// Use a different shape for merge ops
			for(TaskElement te : ob.getTEs()){
				if(te.getAnn() != null && te.getAnn().equals(SDGAnnotation.COLLECTION)){
					/*	output.add(""+ob.getId()+" [shape=polygon,sides=5];\n"); */
					Node tmp1 = graph.createNode(ob.getId()+"");
					tmp1.setLabel(ob.getId()+"")
						.setSize(20)
						.getAttributeValues()
							.addValue(stateAttr, "0")
							.addValue(taskAttr, ob.getTE().getOpName())
							.addValue(idAttr, ob.getTE().getAnn().name())
							.addValue(workflowIdAttr, ob.getWorkflowId()+"")
							.addValue(globalAttr, false+"");
					tmp1.getShapeEntity().setNodeShape(NodeShape.TRIANGLE);
					
					nodeTable.put(ob.getId(), tmp1);
				}
			}
		}
		
		
		
//		
//		Node gephi = graph.createNode("0");
//		gephi
//			.setLabel("Gephi")
//			.setSize(20)
//			.getAttributeValues()
//				.addValue(attUrl, "http://gephi.org")
//				.addValue(attIndegree, "1");
//		gephi.getShapeEntity().setNodeShape(NodeShape.DIAMOND).setUri("GephiURI");
//		
//		Node webatlas = graph.createNode("1");
//		webatlas
//			.setLabel("Webatlas")
//			.getAttributeValues()
//				.addValue(attUrl, "http://webatlas.fr")
//				.addValue(attIndegree, "2");
//		
//		Node rtgi = graph.createNode("2");
//		rtgi
//			.setLabel("RTGI")
//			.getAttributeValues()
//				.addValue(attUrl, "http://rtgi.fr")
//				.addValue(attIndegree, "1");
//		
//		Node blab = graph.createNode("3");
//		blab
//			.setLabel("BarabasiLab")
//			.getAttributeValues()
//				.addValue(attUrl, "http://barabasilab.com")
//				.addValue(attIndegree, "1")
//				.addValue(attFrog, "false");
//		
//		gephi.connectTo("0", webatlas);
//		gephi.connectTo("1", rtgi);
//		webatlas.connectTo("2", gephi);
//		rtgi.connectTo("3", webatlas);
//		gephi.connectTo("4", blab);

		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File f = new File(filename+".gexf");
		Writer out;
		try {
			out =  new FileWriter(f, false);
			graphWriter.writeToStream(gexf, out, "UTF-8");
			System.out.println(f.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
