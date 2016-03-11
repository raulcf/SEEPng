package uk.ac.imperial.lsds.seep.mdflang.functions;

import java.util.function.Predicate;

public class SeepPredicate<T> implements SeepFunctionalTask {

	private Predicate<T> predicate;
	private T arg;
	private boolean answer;

	@Override
	public void apply() {
		answer = predicate.test(arg);
	}
	
}
