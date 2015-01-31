import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.Java.Modifiers;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.Java.Type;
import org.codehaus.janino.Java.FunctionDeclarator.FormalParameter;
import org.codehaus.janino.Java.FunctionDeclarator.FormalParameters;
import org.codehaus.janino.Scanner.Token;

import uk.ac.imperial.lsds.seep.api.annotations.Collection;

public class CustomCollectionParser extends Parser {

	public CustomCollectionParser(Scanner scanner) {
		super(scanner);
		// TODO Auto-generated constructor stub
	}

	@Override
	public MethodDeclarator parseMethodDeclarationRest(
			String optionalDocComment, Modifiers modifiers, Type type,
			String name) throws CompileException, IOException {
		MethodDeclarator tmp = null;

		
		return super.parseMethodDeclarationRest(optionalDocComment,modifiers, type, name);

	}
	
	@Override
	public String[] parseQualifiedIdentifier() throws CompileException,
			IOException {
		List<String> l = new ArrayList();
		l.add(this.readIdentifier());
		while (this.peek(".") && this.peekNextButOne().type == Token.IDENTIFIER) {
			this.read();
			String tmp =this.readIdentifier();
			System.out.println("Recieved: "+ tmp);
			if(tmp != null)
				l.add(tmp);

		}
		return (String[]) l.toArray(new String[l.size()]);
	}
	
	
	
	/**
     * @return The value of the next token, which is an indentifier
     * @throws CompileException The next token is not an identifier
     * 
     * The modification here aims to remove @Collection Annotation from method Declaration
     */
	@Override
    public String readIdentifier() throws CompileException, IOException {
        Token t = this.read();
        System.out.println(t.toString());
        if(t.toString().contains( "@") && this.peekRead("Collection")){
        	System.out.println("Finally Found Collection Annotation! Line: "+ t.getLocation().getLineNumber() + " Val: "+ t.value + t.toString());
        	//this.read();
        	return this.read().value;
        }
        else if (t.type != Token.IDENTIFIER) throw this.compileException("Identifier expected instead of '" + t.value + "'");
        return t.value;
    }

}
