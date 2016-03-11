package uk.ac.imperial.lsds.seep.mdflang.functions;

import java.util.function.Predicate;


public class FTask<T> implements FunctionalTaskWrapper {

	private Predicate<?> pred;
	
	private FTaskType type;
	
	public FTask(Predicate<T> predicate) {
		this.type = FTaskType.PREDICATE;
		this.pred = predicate;
	}
	
}
