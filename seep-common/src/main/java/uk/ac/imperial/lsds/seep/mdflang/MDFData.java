package uk.ac.imperial.lsds.seep.mdflang;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface MDFData<T> extends Explorable {
	
	public MDFData<T> filter(Predicate<T> predicate);
	
	public <O> MDFData<O> map(Function<T, ? extends O> mapper);
	
	public <O> MDFData<O> flatMap(Function<T, ? extends O> mapper);
	
	public void forEach(Consumer<? super T> action);
	
//	Collection<T> data;
//	
//	public MDFData() {
//		data = new ArrayList<>();
//	}
//	
//	public MDFData(Collection<T> data) {
//		this.data = data;
//	}
//	
//	public void add(T t) {
//		data.add(t);
//	}
//	
//	public MDFData<T> filter(Predicate<T> predicate) {
//		MDFData<T> output = new MDFData<T>();
//		for(T t : data) {
//			if(predicate.test(t)){
//				output.add(t);
//			}
//		}
//		return output;
//	}
//
//	public <O> MDFData<O> map(Function<T, ? extends O> mapper) {
//		MDFData<O> output = new MDFData<O>();
//		for(T t : data) {
//			O o = mapper.apply(t);
//			output.add(o);
//		}
//		return output;
//	}
//
//	public <O> MDFData<O> flatMap(Function<T, ? extends O> mapper) {
//		MDFData<O> output = new MDFData<O>();
//		for(T t : data) {
//			O o = mapper.apply(t);
//			output.add(o);
//		}
//		return output;
//	}
//	
//	public void forEach(Consumer<? super T> action) {
//		for(T t : data) {
//			action.accept(t);
//		}
//	}
	
//	public <O> Exploration<MDFData<O>> explore();
	
}
