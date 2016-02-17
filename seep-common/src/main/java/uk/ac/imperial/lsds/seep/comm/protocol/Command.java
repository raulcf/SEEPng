package uk.ac.imperial.lsds.seep.comm.protocol;

public class Command implements SeepCommand {

	private SeepCommand command;
	private short type;
	
	public Command() {} // Kryo serialization
	
	public Command(SeepCommand command) {
		this.command = command;
		this.type = command.familyType();
	}
	
	@Override
	public short familyType() {
		return type;
	}
	
	public SeepCommand getCommand(){
		return command;
	}
	
}
