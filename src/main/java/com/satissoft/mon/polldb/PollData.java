package com.satissoft.mon.polldb;



public interface PollData extends LevelDBKV{
	public Comparable getComparableId();
	public Long getTime();
}
