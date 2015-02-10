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
package uk.ac.imperial.lsds.java2sdg.bricks;

public enum SDGAnnotation{
	PARTITIONED ("Partitioned"), 
	PARTIAL_STATE ("PartialState"), 
	PARTIAL_DATA ("PartialData"), 
	GLOBAL ("Global"),
	GLOBAL_WRITE ("GlobalWrite"),
	GLOBAL_READ ("GlobalRead"),
	COLLECTION ("Collection"),
	LOCAL ("Local"),
	FILE ("File"),
	NETWORKSOURCE ("NetworkSource"),
	CONSOLESINK ("ConsoleSink");
	
	
	private final String value;
	
	private SDGAnnotation(String v) {
		value = v;
	}
	
	public String getValue(){
		return value;
	}
	
	public static SDGAnnotation getAnnotation(String value) {
		if(value.equalsIgnoreCase("Partial"))
			return SDGAnnotation.PARTIAL_STATE;
		
        for(SDGAnnotation v : values())
            if(v.getValue().equalsIgnoreCase(value)) 
            	return v;
        throw new IllegalArgumentException();
    }
	
}
