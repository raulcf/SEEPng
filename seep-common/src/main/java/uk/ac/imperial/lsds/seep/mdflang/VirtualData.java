package uk.ac.imperial.lsds.seep.mdflang;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import uk.ac.imperial.lsds.seep.mdflang.functions.FTask;
import uk.ac.imperial.lsds.seep.mdflang.functions.FunctionalTaskWrapper;

public class VirtualData<T> implements MDFData<T> {

	List<FunctionalTaskWrapper> functions;
	
	@Override
	public MDFData<T> filter(Predicate<T> predicate) {
		
		FunctionalTaskWrapper f = new FTask(predicate);
		
		return this;
	}

	@Override
	public <O> MDFData<O> map(Function<T, ? extends O> mapper) {
		
		// TODO: register this function somehow
		
		return (VirtualData<O>) this;
	}

	@Override
	public <O> MDFData<O> flatMap(Function<T, ? extends O> mapper) {
		
		// TODO: register this function somehow

		return (VirtualData<O>) this;
	}

	@Override
	public void forEach(Consumer<? super T> action) {
		// TODO Auto-generated method stub
		
	}

}
