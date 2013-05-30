package com.satissoft.mon.polldb;

import java.io.Serializable;

public class SimpleEvent implements Serializable{
	long up;
	long down;
	long param;
	int severity;
	int status;
	boolean isAlarm;
	public long getUp() {
		return up;
	}
	public void setUp(long up) {
		this.up = up;
	}
	public long getDown() {
		return down;
	}
	public void setDown(long down) {
		this.down = down;
	}
	public long getParam() {
		return param;
	}
	public void setParam(long param) {
		this.param = param;
	}
	public int getSeverity() {
		return severity;
	}
	public void setSeverity(int severity) {
		this.severity = severity;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public boolean isAlarm() {
		return isAlarm;
	}
	public void setAlarm(boolean isAlarm) {
		this.isAlarm = isAlarm;
	}
	public SimpleEvent(long up, long down, long param, int severity,
			int status, boolean isAlarm) {
		super();
		this.up = up;
		this.down = down;
		this.param = param;
		this.severity = severity;
		this.status = status;
		this.isAlarm = isAlarm;
	}
	
}
