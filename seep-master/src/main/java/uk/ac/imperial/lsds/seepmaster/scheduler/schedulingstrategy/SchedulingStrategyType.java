package uk.ac.imperial.lsds.seepmaster.scheduler.schedulingstrategy;


public enum SchedulingStrategyType {
	
	SEQUENTIAL((short)0, new SequentialSchedulingStrategy()),
	RANDOM((short)1, new RandomSchedulingStrategy()),
	PARALLEL_LOCALITY((short)2, new ParallelLocalitySchedulingStrategy()),
	MDF((short)3, new MDFSchedulingStrategy());
	
	private int type;
	private SchedulingStrategy strategy;
	
	SchedulingStrategyType(int type, SchedulingStrategy strategy){
		this.type = type;
		this.strategy = strategy;
	}
	
	public int ofType(){
		return type;
	}
	
	public static SchedulingStrategy clazz(int type){
		for(SchedulingStrategyType sst : SchedulingStrategyType.values()){
			if(sst.ofType() == type){
				return sst.strategy;
			}
		}
		return null;
	}
}
