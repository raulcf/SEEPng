package uk.ac.imperial.lsds.java2sdg.analysis;

import java.io.IOException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.Scanner.Token;

public class CustomParser extends Parser {

	public CustomParser(Scanner s){
		super(s);
	}
	
	/**
     * @return The value of the next token, which is an indentifier
     * @throws CompileException The next token is not an identifier
     * 
     * The modification here aims to consume @Collection Annotation from method identifiers
     * to avoid Parsing exceptions! Thats the only way to collect method annotations!
     */
	@Override
    public String readIdentifier() throws CompileException, IOException {
        Token t = this.read();
        if(t.toString().contains( "@") && this.peekRead("Collection")){
        	//System.out.println("Found Collection Annotation at Line: "+ t.getLocation().getLineNumber());
        	AnnotationAnalysis.addCustomParserCollectionAnnotation(t.getLocation().getLineNumber(), "Collection");
        	return this.read().value;
        }
        
        if (t.type != Token.IDENTIFIER) throw this.compileException("Identifier expected instead of '" + t.value + "'");
        
        return t.value;
    }

}
