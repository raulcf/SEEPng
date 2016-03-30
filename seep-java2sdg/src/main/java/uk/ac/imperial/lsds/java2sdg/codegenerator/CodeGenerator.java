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
package uk.ac.imperial.lsds.java2sdg.codegenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGRepr;

public class CodeGenerator {

	private final static Logger LOG = LoggerFactory.getLogger(CodeGenerator.class.getCanonicalName());
	
	public static SDGRepr assemble(SDGRepr sdg){
		SDGRepr assembledSDG = CodeGenerator.assembleTE(sdg);
		return assembledSDG;
	}
	
	private static SDGRepr assembleTE(SDGRepr sdg){
		
		for(SDGNode node : sdg.getSdgNodes()){
			String builtCode = null;
			/*  Multi-TE case */
			if(node.getTaskElements().size() > 1){
				builtCode = SeepOperatorNewTemplate.getCodeForMultiOp(node.getTaskElements());
			}
			/*  Single-TE case */
			else if (node.getTaskElements().size() == 1){
				builtCode = SeepOperatorNewTemplate.getCodeForSingleOp(node.getTaskElements().values().iterator().next());
			}
			else{
				LOG.error("SDGRepr with empty TaskElement List!");
			}
			try {
				if(builtCode == null){
					LOG.error("CodeGenerator failed to produce any Code for Node {}", node.getName());
				}
				else{
					System.out.println("CODE PRODUCED: "+ builtCode);
					/* Maybe add the code to the SDG node here? */
					//op.setCode(builtCode);
				}
			} 
			catch (Exception e) {
				e.printStackTrace();
				LOG.error("Invalid code assigment: "+e.getMessage());
			}
		}
		return sdg;
	}
	
}
