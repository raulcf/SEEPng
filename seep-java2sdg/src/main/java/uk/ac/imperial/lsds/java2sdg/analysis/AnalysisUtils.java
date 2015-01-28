package uk.ac.imperial.lsds.java2sdg.analysis;

import org.codehaus.janino.Java.Annotation;
import org.codehaus.janino.Java.Type;

import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;

public class AnalysisUtils {

	public SDGAnnotation identifyAnnotation(Annotation annotation){
		Type type = annotation.getType();
		String typeStr = type.toString();
		
		// FIXME: get names and types from the enum so that this is always automatic
		
		if(typeStr.equals("Partitioned")){
			return SDGAnnotation.PARTITIONED;
		}
		else if(typeStr.equals("Partial")){
			return SDGAnnotation.PARTIAL_STATE;
		}
		else if(typeStr.equals("PartialState")){
			return SDGAnnotation.PARTIAL_STATE;
		}
		else if(typeStr.equals("PartialData")){
			return SDGAnnotation.PARTIAL_DATA;
		}
		else if(typeStr.equals("Global")){
			return SDGAnnotation.GLOBAL;
		}
		else if(typeStr.equals("Collection")){
			return SDGAnnotation.COLLECTION;
		}
		else if(typeStr.equals("Collection")){
			return SDGAnnotation.COLLECTION;
		}
		else if(typeStr.equals("NetworkSource")){
			return SDGAnnotation.NETWORKSOURCE;
		}
		else if(typeStr.equals("File")){
			return SDGAnnotation.FILE;
		}
		else if(typeStr.equals("ConsoleSink")){
			return SDGAnnotation.CONSOLESINK;
		}
		return null;
	}
	
}
