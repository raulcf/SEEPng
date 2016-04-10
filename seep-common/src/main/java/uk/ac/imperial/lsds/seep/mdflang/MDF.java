package uk.ac.imperial.lsds.seep.mdflang;

import java.util.Collection;
import java.util.function.Function;


public class MDF {

	public static <T> MDFData<T> fromCollection(Collection<T> data) {
		return new RealData<T>(data);
	}
	
	public static <T> void print(MDFData<T> data) {
		data.forEach((T t) -> {
			System.out.println(t);
		}); 
	}
	
	public static Exploration EXPLORE(MDFData data, Function func) {
		
		
		return null;
	}
	
}
