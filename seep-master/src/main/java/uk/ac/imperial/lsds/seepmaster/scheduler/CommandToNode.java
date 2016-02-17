package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;

public class CommandToNode {
	public CommandToNode(SeepCommand command, Connection c){
		this.command = command;
		this.c = c;
	}
	public SeepCommand command;
	public Connection c;
}