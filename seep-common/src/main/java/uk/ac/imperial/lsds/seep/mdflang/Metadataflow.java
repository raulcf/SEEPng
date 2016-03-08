package uk.ac.imperial.lsds.seep.mdflang;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class Metadataflow {

	public <I extends Explorable, O extends MDFData<?>> Exploration<MDFData<?>> explore(Function<I, O> func, List<I> params) {
		List<O> output = new ArrayList<>();
		for(I i : params) {
			O d = func.apply(i);
			output.add(d);
		}
		return (Exploration<MDFData<?>>) output;
	}
	
	public <I extends Chooseable, O extends I> I choose(List<I> input, Predicate<I> evaluator) {
		for(I i : input) {
			if(evaluator.test(i)) return i;
		}
		return null;
	}
	
	public <I extends Chooseable, O extends I> List<I> chooseTopK(List<I> input, Function<I, Double> ranker) {
		List<I> il = new ArrayList<>();
		for(I i : input) {
			if(ranker.apply(i) == 9) {
				il.add(i);
			}
		}
		return il;
	}
	
	public <A,B,C,D,E,F,G> G check(A a, B b, C c, D d, E e, F f) {
		G g = null;
		//
		return g;
	}
	
}
