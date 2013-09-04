package com.satissoft.mon.polldb;



public interface PollData extends LevelDBKV{
	@SuppressWarnings("rawtypes")
	public Comparable getComparableId();
	public Long getTime();
}
