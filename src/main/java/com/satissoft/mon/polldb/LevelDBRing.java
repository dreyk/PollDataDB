package com.satissoft.mon.polldb;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class LevelDBRing {
	private LevelDBPartition partitions[];
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	int archiveDeps;
	long partitionSize;
	File dataRoot;
	Timer resourceTimer;  
	private static long clearResourceTime = 1000l*60l*5l;
	private boolean sync;
	public LevelDBRing(File dataRoot,boolean sync,long partitionSize,int archiveDeps){
		dataRoot.mkdirs();
		this.sync = sync;
		this.archiveDeps = archiveDeps;
		this.partitionSize = partitionSize;
		this.dataRoot = dataRoot;
		long curPartiotion = partition(System.currentTimeMillis());
		long firstAvailable = curPartiotion-archiveDeps+1; 
		partitions = new LevelDBPartition[archiveDeps];
		for(int i = 0 ; i < partitions.length ;i++){
			partitions[i]  = new LevelDBPartition(dataRoot,sync,firstAvailable+i);
		}
		resourceTimer = new Timer("leveldb clear timer");
		resourceTimer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				clear();
				
			}
		},clearResourceTime,clearResourceTime);
	}
	private void clear(){
		List<LevelDBPartition>toDel = new ArrayList<LevelDBPartition>();
		WriteLock wLock = lock.writeLock();
		wLock.lock();
		try{
			long expired = partition(System.currentTimeMillis())-archiveDeps; 
			int rm = 0;
			for(int i = 0 ; i < partitions.length ; i++){
				LevelDBPartition in = partitions[i];
				if(in.getPartition()<expired){
					toDel.add(in);
					rm++;
				}
				else
					break;
			}
			if(rm<1){
				return;
			}
			else if(rm<partitions.length){
				partitions = Arrays.copyOfRange(partitions,rm,partitions.length);
			}
			else{
				long curPartiotion = partition(System.currentTimeMillis());
				long firstAvailable = curPartiotion-archiveDeps+1; 
				partitions = new LevelDBPartition[archiveDeps];
				for(int i = 0 ; i < partitions.length ;i++){
					partitions[i]  = new LevelDBPartition(dataRoot,sync,firstAvailable+i);
				}
			}
		}
		finally{
			wLock.unlock();
		}
		for(LevelDBPartition d:toDel){
			d.remove();
		}
	}
	public long getMinTime(){
		return (partition(System.currentTimeMillis())-archiveDeps+1)*partitionSize;
	}
	public long getMaxTime(){
		return (partition(System.currentTimeMillis())+2)*partitionSize;
	}
	public LevelDBPartition getPartiotion(long partition) throws PollDataDBException{
		LevelDBPartition instance = null;
		ReadLock rLock = lock.readLock();
		rLock.lock();
		try{
			int index = (int)(partition-partitions[0].getPartition());
			if(index<0){
				partition = partitions[0].getPartition();
				instance = partitions[0];
			}
			else if(index<partitions.length)
				instance = partitions[index];
		}
		finally{
			rLock.unlock();
		}
		if(instance!=null && instance.isStarted())
			return instance;
		else{
			return initPartition(partition);
		}
		
	}
	public long partition(long time){
		return (long)(time/partitionSize);
	}
	private LevelDBPartition initPartition(long partition) throws PollDataDBException{
		WriteLock wLock = lock.writeLock();
		wLock.lock();
		try{
			long firt = partitions[0].getPartition();
			int index = (int)(partition-partitions[0].getPartition());
			if(index >= partitions.length){
				int oldLength = partitions.length;
				partitions = Arrays.copyOf(partitions,index+1);
				for(int i = oldLength; i < partitions.length ; i++){
					partitions[i] = new LevelDBPartition(dataRoot,sync,(firt+(long)i));
				}
			}
			LevelDBPartition instance = partitions[index];
			if(!instance.isStarted())
				instance.start();
			return instance;
		}
		finally{
			wLock.unlock();
		}
	}
	public void close(){
		resourceTimer.cancel();
		WriteLock wLock = lock.writeLock();
		wLock.lock();
		try{
			for(LevelDBPartition instance:partitions){
				instance.stop();
			}
			partitions = null;
		}
		finally{
			wLock.unlock();
		}
	}
}
