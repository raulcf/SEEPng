package uk.ac.imperial.lsds.seep.mdflang;

@FunctionalInterface
public interface XMDFData<T> {

	void testt();
	
	default XMDFData<T> test() {
		
		return null;
	}
}
