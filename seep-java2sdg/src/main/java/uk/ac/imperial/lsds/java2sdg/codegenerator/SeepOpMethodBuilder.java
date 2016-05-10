package uk.ac.imperial.lsds.java2sdg.codegenerator;

import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.CannotCompileException;
import javassist.CtClass;

/**
 * @author pg1712
 *
 */
public class SeepOpMethodBuilder {
	
	
	public static CtMethod genBaseCompose(CtClass cc, String code) throws CannotCompileException{
		CtMethod compose = CtNewMethod.make("public SeepLogicalQuery compose() {" + code + "}", cc);
		return compose;
	}
	
	public static CtMethod genProcessorMethod(CtClass cc, String code) throws CannotCompileException{
		CtMethod processDataSingle = CtNewMethod
				.make("public void processData(ITuple data, API api) {" + code + "}", cc);
		return processDataSingle;
	}
	
	public static CtMethod genProcessorGroupMethod(CtClass cc, String code) throws CannotCompileException {
		CtMethod processDataSingle = CtNewMethod
				.make("public void processDataGroup (List data, API api) {" + code + "}", cc);
		return processDataSingle;
	}
	
	public static CtMethod genSetupMethod(CtClass cc, String code) throws CannotCompileException {
		CtMethod setup = CtNewMethod.make("public void setUp() {"+ code +"}", cc);
		return setup;
	}
	
	public static CtMethod genCloseMethod(CtClass cc, String code) throws CannotCompileException {
		CtMethod close = CtNewMethod.make("public void close() {"+ code +"}", cc);
		return close;
	}
	
	public static CtMethod genClassMethod(CtClass cc, String code) throws CannotCompileException {
		CtMethod aMethod = CtNewMethod.make(code, cc);
		return aMethod;
	}

}
