package uk.ac.imperial.lsds.seep.mdflang;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface X<I extends BiFunction<I,P,O>, P extends Iterable<?>, O extends Function<I,O>> {

	default List<Function<I,O>> explore (BiFunction<I,P,O> userFunc, Iterable<P> params) {
		List<Function<I,O>> output = null;
		for(P p : params) {
			I i = null;
			Function<I,O> f = userFunc.apply(i, p);
			output.add(f);
		}
		return output;
	}
}
