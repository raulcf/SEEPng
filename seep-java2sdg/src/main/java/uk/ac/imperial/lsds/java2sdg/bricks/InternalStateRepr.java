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

import org.codehaus.janino.Java.Type;

public class InternalStateRepr {

	private final int seId;
	private final String name; 
	private final Type stateClass;
	private final SDGAnnotation annotationType;
	
	public InternalStateRepr(int stateId, Type stateType, String name, SDGAnnotation annotationType){
		this.seId = stateId;
		this.name = name;
		this.stateClass = stateType;
		this.annotationType = annotationType;
	}
	
	public int getStateId(){
		return seId;
	}
	
	public String getName(){
		return name;
	}
	
	public Type getStateType(){
		return stateClass;
	}
	
	public SDGAnnotation getStateAnnotation(){
		return annotationType;
	}
}
