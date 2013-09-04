package com.satissoft.mon.polldb;

import java.io.Serializable;

public class SimpleStats implements Serializable{
	private long time;
	private int state;
	public SimpleStats(long time, int state) {
		super();
		this.time = time;
		this.state = state;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public int getState() {
		return state;
	}
	public void setState(int state) {
		this.state = state;
	}
}
