import java.io.FileReader;
import java.io.IOException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.util.Traverser;


public class LVAProof extends Traverser {

	public static void main(String args[]) throws CompileException, IOException {
		LVAProof lva = new LVAProof();
		for (String fileName : args) {
		    // Parse each compilation unit.
		    FileReader r = new FileReader(fileName);
		    Java.CompilationUnit cu;
		    try {
		        cu = new Parser(new Scanner(fileName, r)).parseCompilationUnit();
		    } 
		    finally {
		        r.close();
		    }
		    // Traverse it and count declarations.
		    lva.traverseCompilationUnit(cu);
		}
	}
	
	@Override 
	public void traverseLocalVariableDeclarationStatement(Java.LocalVariableDeclarationStatement lvds){
		Location l = lvds.getLocation();
		System.out.println("LOCATION: ");
		System.out.println(l.getLineNumber());
		
		System.out.println("LOCAL-VARIABLE: ");
		System.out.println(lvds.toString());

		Java.Modifiers m = lvds.modifiers;
		System.out.println("MODIFIER: ");
		
	}
}
