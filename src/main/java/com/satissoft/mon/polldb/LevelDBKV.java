package com.satissoft.mon.polldb;

public interface LevelDBKV {
	public byte[] getLevelDBKey();
	public byte[] getLevelDBValue();
	public  LevelDBKV  initLevelDb(byte key[],byte data[]);
	
}
