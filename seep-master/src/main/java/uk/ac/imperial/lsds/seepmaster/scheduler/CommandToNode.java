package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;

public class CommandToNode {
	public CommandToNode(MasterWorkerCommand command, Connection c){
		this.command = command;
		this.c = c;
	}
	public MasterWorkerCommand command;
	public Connection c;
}