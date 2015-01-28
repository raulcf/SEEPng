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
import java.util.Vector;

import uk.ac.imperial.lsds.seep.api.annotations.Partitioned;
import uk.ac.imperial.lsds.seep.api.annotations.Partial;
import uk.ac.imperial.lsds.seep.api.annotations.Global;
import uk.ac.imperial.lsds.seep.api.annotations.Collection;
import uk.ac.imperial.lsds.seep.api.annotations.Any;
import uk.ac.imperial.lsds.seep.api.annotations.DriverProgram;
import uk.ac.imperial.lsds.seep.api.data.Type.JavaType;
import uk.ac.imperial.lsds.seep.api.annotations.File;
import uk.ac.imperial.lsds.seep.api.annotations.Schema;
import uk.ac.imperial.lsds.seep.api.annotations.Network;


public class UT1 implements DriverProgram{

	@Partial
	public Vector weights = new Vector();
	private static final int ITER = 5;
	private static final double DELTA = 0.3;

	public void main(){
		@File(path = "/data/test.txt")
		@Schema(fieldname = {"userId, ts, mail"}, types = {JavaType.INT, JavaType.LONG, JavaType.STRING})
		Vector items = null; // get data somehow
		train(items); // call function -> implies this is an entry point
		@Network(endpoint = "localhost:5000")
		@Schema(fieldname = {"userId, ts, mail"}, types = {JavaType.INT, JavaType.LONG, JavaType.STRING})
		int item = 5; // a simple item to classify;
		test(item);
	}

	public void train(Vector items){
		for (int i = 0; i < ITER; i++) {
			for (int j = 0; j<items.size(); j++) {
				int t = i;
				double predicted = classify(t, weights);
				Vector gradient = new Vector();
				for (int k = 0; k < items.size(); k++){
					int f = k;
					int label = 1;
					gradient.add(f * (label - predicted) * DELTA);
					@PartialData weights = add(@Global weights, gradient);
				}
			}
			Vector gWeights = merge(weights);
			assign(@Global weights, gWeights);
		}
	}

	public boolean test(int item){
		double result = classify(item, weights);
		boolean toReturn = false;
		if(result > 0){
			toReturn = true;
		}
		return toReturn;
	}

	private boolean assign(Vector weights, Vector gWeights){
		weights = gWeights;
		return true;
	}

	private double classify(int t, Vector weights){
		return 0;
	}

	private Vector add(Vector weights, Vector gradient){
		return gradient;
	}

	private Vector merge(@Collection Vector weights){
		return weights;
	}

}
