package uk.ac.imperial.lsds.seep.mdflang.functions;

import java.util.function.Function;

public class SeepFunction<T,O> implements SeepFunctionalTask {

	private Function<T,O> func;
	private T input;
	private O output;
	
	public void apply() {
		output = func.apply(input);
	}
	
}
