package uk.ac.imperial.lsds.seep.mdflang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class RealData<T> implements MDFData<T> {

	Collection<T> data;
	
	public RealData() {
		data = new ArrayList<>();
	}
	
	public RealData(Collection<T> data) {
		this.data = data;
	}
	
	public void add(T t) {
		data.add(t);
	}
	
	public MDFData<T> filter(Predicate<T> predicate) {
		RealData<T> output = new RealData<T>();
		for(T t : data) {
			if(predicate.test(t)){
				output.add(t);
			}
		}
		return output;
	}

	public <O> MDFData<O> map(Function<T, ? extends O> mapper) {
		RealData<O> output = new RealData<O>();
		for(T t : data) {
			O o = mapper.apply(t);
			output.add(o);
		}
		return output;
	}

	public <O> MDFData<O> flatMap(Function<T, ? extends O> mapper) {
		RealData<O> output = new RealData<O>();
		for(T t : data) {
			O o = mapper.apply(t);
			output.add(o);
		}
		return output;
	}
	
	public void forEach(Consumer<? super T> action) {
		for(T t : data) {
			action.accept(t);
		}
	}
}
