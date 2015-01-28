package uk.ac.imperial.lsds.seep.api.state;

/**
 * A class implements Migratable to indicate that its state can travel to different ExecutionUnits (regardless localization)
 * A class that is Migratable is always Mergeable, i.e. it can receive instances from other ExecutionUnits as well. The
 * particular strategy to perform when receiving remote instances is application-dependent, as is the normal case of a 
 * Mergeable class.
 * @author ra
 *
 */
public interface Migratable extends Mergeable {

	public boolean migrate();
	
}
