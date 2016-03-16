package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import uk.ac.imperial.lsds.seep.api.data.Type;


public class VariableRepr {

	private final Type type;
	private final String name;
	
	private VariableRepr(Type type, String name){
		this.type = type;
		this.name = name;
	}
	
	public static VariableRepr var(Type type2, String name){
		return new VariableRepr(type2, name);
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
