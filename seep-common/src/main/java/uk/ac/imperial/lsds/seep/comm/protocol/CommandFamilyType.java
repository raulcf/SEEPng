package uk.ac.imperial.lsds.seep.comm.protocol;

public enum CommandFamilyType {

	MASTERCOMMAND((short)0),
	WORKERCOMMAND((short)1);
	
	private short type;
	
	CommandFamilyType(short type) {
		this.type = type;
	}
	
	public short ofType() {
		return this.type;
	}
	
}
