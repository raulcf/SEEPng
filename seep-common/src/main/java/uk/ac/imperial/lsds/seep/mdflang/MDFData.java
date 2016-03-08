package uk.ac.imperial.lsds.seep.mdflang;

import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

public interface MDFData<T> {
	
	public MDFData<T> filter(Predicate<? super T> predicate);

	public <O> MDFData<O> map(Function<? super T, ? extends O> mapper);

	public <O> MDFData<O> flatMap(Function<? super T, ? extends MDFData<? extends O>> mapper);

	public T reduce(T identity, BinaryOperator<T> accumulator);
	
	public <O> Exploration<MDFData<O>> explore();
	
}
