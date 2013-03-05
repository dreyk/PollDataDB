package com.satissoft.mon.polldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;



import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LevelDBPartition implements Runnable{
	private static Logger log = LoggerFactory.getLogger(LevelDBPartition.class);
	private File dataRoot;
	private long partition;
	public long getPartition() {
		return partition;
	}
	private String name;
	private DB db;
	private boolean isStarted = false;
	public boolean isStarted() {
		return isStarted;
	}
	private int state = -1;
	private ArrayBlockingQueue<PollDataDBTask> tasks;
	private boolean sync;
	public LevelDBPartition(File dataRoot,boolean sync,long partition){
		this.sync = sync;
		this.dataRoot = dataRoot;
		this.partition = partition;
		this.name = "polldata-"+dataRoot.getName()+"."+partition;
		this.tasks = new ArrayBlockingQueue<PollDataDBTask>(10000);
	}
	public void execute(PollDataDBTask task,long timeout,TimeUnit unit) throws InterruptedException{
		tasks.offer(task, timeout, unit);
	}
	public void start(){
		Thread hostedThread = new Thread(this,name);
		isStarted = true;
		hostedThread.start();
		
	}
	public void stop(){
		isStarted = false;
	}
	public void remove(){
		if(isStarted)
			tasks.offer(new RemoveTask());
	}
	public void run(){
		Options options = new Options();
		options.createIfMissing(true);
		try {
			db = factory.open(new File(dataRoot,Long.toString(partition)), options);
			log.debug(name+" instace started.");
			state = 1;
		} catch (IOException e1) {
			e1.printStackTrace();
			log.error(name+" Can't start.",e1);
			state = 0;
			
		}
		while(isStarted){
			PollDataDBTask task;
			try {
				while((task=tasks.poll(1,TimeUnit.SECONDS))!=null && isStarted){
					executeTask(task);
				}
			} catch (InterruptedException e) {
				log.error("worker thread for "+name+" interrupted",e);
			}
		}
		try {
			doStop();
		} catch (IOException e) {
			log.error("Can't stop leveldb correct.",e);
		}
	}
	private void doRemove(){
		isStarted = false;
		try {
			db.close();
		} catch (IOException e) {
			log.error("Can't correct close partition "+name,e);
		}
		File dir = new File(dataRoot,Long.toString(partition));
		deleteFile(dir);
		log.info(name + " deleted");
		state = -1;
	}
	private void doStop() throws IOException{
		if (isStarted && state > -1) {
			log.debug(name + " close levedb.");
			state = -1;
			db.close();
		}
	}
	private void store(StorePollDataTask task){
		int count = 0;
		WriteBatch batch = null;
		try {
			WriteOptions wr = new WriteOptions();
			wr = wr.sync(sync);
			batch = db.createWriteBatch();
			for(LevelDBKV kv:task.getData()){
				batch.put(kv.getLevelDBKey(),kv.getLevelDBValue());
				count++;
			}
			db.write(batch,wr);
		} catch (DBException e) {
			count = 0;
			log.error("Can't write to leveldb",e);
		}
		finally{
			if(db!=null){
				try {
					batch.close();
				} catch (Exception e2) {
					count = 0;
					log.error("Can't close leveldb batch",e2);
				}
			}
		}
		task.commit(count);
	}
	private void read(ScanPollDataTask task){
		final ReadOptions ro = new ReadOptions();
		ro.fillCache(false);
		ro.snapshot(db.getSnapshot());
		DBIterator iterator = db.iterator(ro);
		task.commit(new ScanPollDataReader(iterator));
	}
	private void executeTask(PollDataDBTask task){
		if(task instanceof RemoveTask){
			doRemove();
		}
		if(state!=1){
			task.getResult().delivery(new PollDataDBException("partiotion "+name+" not ready state="+state));
		}
		if(task instanceof StorePollDataTask){
			store((StorePollDataTask)task);
		}
		else if(task instanceof ScanPollDataTask){
			read((ScanPollDataTask)task);
		}
		else
			task.getResult().delivery(new NoSuchMethodException("Uncknown task"));
	}
	private class RemoveTask extends GeneralPollDataDBTask{
		public RemoveTask(){
			super();
		}
	}
	private void deleteFile(File f){
		if(f.isDirectory()){
			for(File c:f.listFiles()){
				if(c.isDirectory()){
					deleteFile(c);
				}
				else
					c.delete();
			}
			f.delete();
		}
		else
			f.delete();
	}
}