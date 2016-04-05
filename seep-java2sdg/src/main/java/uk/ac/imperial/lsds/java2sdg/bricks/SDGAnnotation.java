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
	CONSOLESINK ("ConsoleSink"),
	//Might need to handle the Override Annotation differently(?)
	OVERRIDE ("Override");
	
	
	private final String value;
	
	private SDGAnnotation(String v) {
		value = v;
	}
	
	public String getValue(){
		return value;
	}
	
	public static SDGAnnotation getAnnotation(String value) {
		
		/* After latest code refactor - each Java2SDG programm needs to implement SeepProgram 
		 * and Override configure method - defining workflow IO -
		 * As a result we need to handle that annotation - Just store it for now!
		 */
		
		/* Partial and Partial_State are the SAME Annotation*/
		if(value.equalsIgnoreCase("Partial"))
			return SDGAnnotation.PARTIAL_STATE;
		
        for(SDGAnnotation v : values())
            if(v.getValue().equalsIgnoreCase(value)) 
            	return v;
        throw new IllegalArgumentException();
    }
	
}
