package uk.ac.imperial.lsds.seep.mdflang;

import java.util.Collection;


public class MDF {

	public static <T> MDFData<T> fromCollection(Collection<T> data) {
		return new RealData<T>(data);
	}
	
	public static <T> void print(MDFData<T> data) {
		data.forEach((T t) -> {
			System.out.println(t);
		}); 
	}
	
}
