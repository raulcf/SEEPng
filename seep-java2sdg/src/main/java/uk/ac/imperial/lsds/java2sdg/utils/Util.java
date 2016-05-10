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
package uk.ac.imperial.lsds.java2sdg.utils;

import java.util.logging.Logger;

import soot.Body;
import soot.SootClass;
import soot.SootMethod;
import soot.toolkits.graph.ExceptionalUnitGraph;
import soot.toolkits.graph.UnitGraph;

public class Util {
	
	/* Moving to relative paths rather than static wherever possible*/
	private static String PROJECT_PATH;
	private final static Logger LOG = Logger.getLogger(Util.class.getCanonicalName());

	private Util() {}
	
	public static String getProjectPath(){
		if(Util.PROJECT_PATH == null)
			Util.PROJECT_PATH = System.getProperty("user.dir");
		return Util.PROJECT_PATH;
	}

	public static UnitGraph getCFGForMethod(String methodName, SootClass c) {
		SootMethod m = c.getMethodByName(methodName);
		Body b = m.retrieveActiveBody();
		// Build CFG
		UnitGraph cfg = new ExceptionalUnitGraph(b);
		return cfg;
	}
}
