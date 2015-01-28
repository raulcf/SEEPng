package uk.ac.imperial.lsds.seep.api.state.stateimpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import uk.ac.imperial.lsds.seep.api.state.Checkpoint;
import uk.ac.imperial.lsds.seep.api.state.Mergeable;
import uk.ac.imperial.lsds.seep.api.state.Partitionable;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.api.state.Streamable;
import uk.ac.imperial.lsds.seep.api.state.Versionable;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;

public class SeepMap<K,V> implements Map<K,V>, Checkpoint, Partitionable, Mergeable, Streamable, Versionable, SeepState {

	// Main map
	private int owner;
	private Map<K, V> map;
	
	// Support for dirtyState
	private Map<K, V> dirtyState;
	private Set<K> keysRemoved;
	private AtomicBoolean snapshotMode;
	private ReentrantLock lock;
	
	public SeepMap(){
		this.map = new HashMap<>();
		this.dirtyState = new HashMap<>();
		this.keysRemoved = new HashSet<>();
		this.snapshotMode = new AtomicBoolean(false);
		this.lock = new ReentrantLock();
	}
	
	/** Implement SeepState interface **/
	@Override
	public void setOwner(int owner){
		this.owner = owner;
	}
	
	@Override
	public int getOwner(){
		return owner;
	}
	
	/** Implement Map<K,V> interface **/
	
	@Override
	public int size() {
		if(snapshotMode.get()){
			lock.lock();
			int size = map.size() - keysRemoved.size() + dirtyState.size();
			lock.unlock();
			return size;
		}
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		if(snapshotMode.get()){
			lock.lock();
			boolean empty = map.isEmpty() || dirtyState.isEmpty();
			lock.unlock();
			return empty;
		}
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		if(snapshotMode.get()){
			lock.lock();
			boolean containsKey = (map.containsKey(key) && !keysRemoved.contains(key)) || dirtyState.containsKey(key);
			lock.unlock();
			return containsKey;
		}
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		// FIXME: note how broken is this implementation... who uses containsValue anyway... ainss...
		if(snapshotMode.get()){
			lock.lock();
			boolean containsValue = map.containsValue(value) || dirtyState.containsValue(value);
			lock.unlock();
			return containsValue;
		}
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		if(snapshotMode.get()){
			lock.lock();
			if(dirtyState.containsKey(key)){
				lock.unlock();
				return dirtyState.get(key);
			}
			lock.unlock();
			return map.get(key);
		}
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		lock.lock();
		V v = map.put(key, value);
		lock.unlock();
		return v;
	}

	@Override
	public V remove(Object key) {
		lock.lock();
		V v = map.remove(key);
		lock.unlock();
		return v;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		lock.lock();
		map.putAll(m);
		lock.unlock();
	}

	@Override
	public void clear() {
		lock.lock();
		map.clear();
		lock.unlock();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}
	
	/** Implement Versionable interface **/
	
	@Override
	public void enterSnapshotMode() {
		this.snapshotMode.getAndSet(true);
	}

	@Override
	public void reconcile() {
		if(snapshotMode.get()){
			lock.lock();
			performReconcileOperation();
			lock.unlock();
		}
		else{
			throw new AttemptToReconcileStateNotInSnapshotModeException("An attempt was made to reconcile state not in snapshot mode");
		}
		
	}
	
	private void performReconcileOperation(){
		// Additions and updates
		for(Map.Entry<K, V> dirtyEntry : dirtyState.entrySet()){
			map.put(dirtyEntry.getKey(), dirtyEntry.getValue());
		}
		// Removals
		for(K k : keysRemoved){
			map.remove(k);
		}
		// Clean dirtyState
		dirtyState.clear();
		keysRemoved.clear();
		System.gc();
	}
	
	/** Implement Streamable interface **/

	@Override
	public Iterator<Map.Entry<K, V>> makeStream() {
		if(snapshotMode.get()) throw new IllegalOperationOnStateException("Attemp to stream state while in snapshot mode");
		return map.entrySet().iterator();
	}
	
	/** Implement Mergeable interface **/

	@SuppressWarnings("unchecked")
	@Override
	public void merge(List<SeepState> state) {
		if(snapshotMode.get()) throw new IllegalOperationOnStateException("Attemp to stream state while in snapshot mode");
		for(SeepState chunk : state){
			if(!(chunk instanceof SeepMap)){
				throw new IncompatibleStateException("Attempt to merge an incompatible state");
			}
			for(Map.Entry<K, V> entry : ((SeepMap<K,V>)chunk).entrySet()){
				map.put(entry.getKey(), entry.getValue());
			}
		}
		
	}
	
	/** Implement Partitionable interface **/

	@Override
	public List<SeepState> partition(){
		List<SeepState> partitions = new ArrayList<>();
		int partitioningKey = Integer.MAX_VALUE / 2;
		Map<K,V> partition1 = new SeepMap<>();
		Map<K,V> partition2 = new SeepMap<>();
		for(K key : map.keySet()){
			if(key.hashCode() < partitioningKey){
				partition1.put(key, map.get(key));
			}
			else{
				partition2.put(key, map.get(key));
			}
		}
		partitions.add((SeepState)partition1);
		partitions.add((SeepState)partition2);
		return partitions;
	}
	
	@Override
	public List<SeepState> partition(int partitions) {
		throw new NotImplementedException("yet to implement n-partitioning...");
	}
	
	/** Implement Checkpoint interface **/

	@Override
	public byte[] checkpoint() {
		// TODO: Auto-generated method stub
		return null;
	}

	@Override
	public void recover(byte[] bytes) {
		// TODO Auto-generated method stub
		
	}

}
