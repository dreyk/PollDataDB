package com.satissoft.mon.polldb;

import java.nio.ByteBuffer;

public class SimplePollData implements PollData{
	public LevelDBKV initLevelDb(byte[] key, byte[] data) {
		SimplePollData i = new SimplePollData();
		ByteBuffer buffer = ByteBuffer.wrap(key);
		i.setId(buffer.getLong());
		i.setTime(buffer.getLong());
		buffer = ByteBuffer.wrap(data);
		i.setSeverity(buffer.getInt());
		i.setStatus(buffer.getInt());
		i.setValue(new String(buffer.array(),buffer.position(),buffer.remaining()));
		return i;
	}
	public byte[] getLevelDBKey() {
		ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.putLong(id);
		buffer.putLong(time);
		return buffer.array();
	}
	public byte[] getLevelDBValue() {
		ByteBuffer buffer = ByteBuffer.allocate(8+value.length());
		buffer.putInt(severity);
		buffer.putInt(status);
		buffer.put(value.getBytes());
		return buffer.array();
	}
	Long id;
	Long time;
	int severity;
	public SimplePollData(){
		
	}
	public SimplePollData(Long id, Long time, int severity, int status, String value) {
		super();
		this.id = id;
		this.time = time;
		this.severity = severity;
		this.status = status;
		this.value = value;
	}
	public Comparable getComparableId() {
		return id;
	}
	int status;
	String value;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Long getTime() {
		return time;
	}
	public void setTime(Long time) {
		this.time = time;
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
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
}
