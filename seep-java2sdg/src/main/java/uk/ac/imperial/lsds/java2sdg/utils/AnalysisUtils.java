package uk.ac.imperial.lsds.java2sdg.utils;

import org.codehaus.janino.Java.Annotation;
import org.codehaus.janino.Java.Type;

import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;

public class AnalysisUtils {

	public SDGAnnotation identifyAnnotation(Annotation annotation){
		Type type = annotation.getType();
		String typeStr = type.toString();
		
		return SDGAnnotation.getAnnotation(typeStr);
	}
	
}
