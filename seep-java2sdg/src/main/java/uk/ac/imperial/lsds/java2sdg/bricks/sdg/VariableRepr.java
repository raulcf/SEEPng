package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import org.codehaus.janino.Java.Type;


public class VariableRepr {

	private final Type type;
	private final String name;
	
	private VariableRepr(Type type, String name){
		this.type = type;
		this.name = name;
	}
	
	public static VariableRepr var(Type janinoType, String name){
		return new VariableRepr(janinoType, name);
	}
	
	public Type getRawType(){
		return type;
	}
	
	public String getType(){
		return type.toString();
	}
	
	public String getName(){
		return name;
	}
	
}
