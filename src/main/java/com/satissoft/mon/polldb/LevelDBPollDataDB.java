package com.satissoft.mon.polldb;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LevelDBPollDataDB implements PollDataDB {
	private LevelDBRing ring;
	private boolean isClosed = true;
	public LevelDBPollDataDB(Properties properties){
		this(properties.getProperty("data_root"),
				Boolean.parseBoolean(properties.getProperty("sync")),
				Long.parseLong(properties.getProperty("partition_size")),
				Integer.parseInt(properties.getProperty("archive_deps")));
	}
	public LevelDBPollDataDB(String dataRoot,boolean sync,long partitionSize,int archiveDeps){
		this(new File(dataRoot),sync,partitionSize,archiveDeps);
	}
	public LevelDBPollDataDB(File dataRoot,boolean sync,long partitionSize,int archiveDeps){
		ring = new LevelDBRing(dataRoot,sync, partitionSize, archiveDeps);
		isClosed = false;
	}
	public StoreResults stote(List<? extends PollData>  datas,long timeout,TimeUnit unit){
		StoreResults res = new StoreResults();
		if(datas==null || datas.size()<1){
			return res;
		}
		if (isClosed) {
			res.addError(datas.size(), new PollDataDBException("DB is closed"));
			return res;
		}

		Collections.sort(datas, new Comparator<PollData>() {
			public int compare(PollData o1, PollData o2) {
				if (o1.getTime() == o2.getTime())
					return o1.getComparableId().compareTo(o2.getComparableId());
				else
					return o1.getTime().compareTo(o2.getTime());
			}

		});
		List<PollData> ldatas = null;
		long prev = -1;
		long minTime = ring.getMinTime();
		long maxTime = ring.getMaxTime();
		for (PollData data : datas) {
			if (data.getTime() < minTime || data.getTime() > maxTime) {
				res.addSkip(1);
				continue;
			}
			long partition = ring.partition(data.getTime());
			if (partition != prev) {
				if (prev != -1) {
					if (ldatas != null && ldatas.size() > 0) {
						try {
							StoreResults tmp = write(partition, ldatas, timeout, unit);
							res.add(tmp);
						} catch (Exception e) {
							e.printStackTrace();
							res.addError(ldatas.size(), e);
						}
					}
				}
				ldatas = new ArrayList<PollData>();
				prev = partition;
			}
			ldatas.add(data);
		}
		if (ldatas != null && ldatas.size() > 0) {
			try {
				StoreResults tmp = write(prev, ldatas, timeout, unit);
				res.add(tmp);
			} catch (Exception e) {
				e.printStackTrace();
				res.addError(ldatas.size(), e);
			}
		}

		return res;
	}
	private StoreResults write(long partition,List<PollData> datas,long timeout,TimeUnit unit) throws PollDataDBException{
		try {
			Collections.sort(datas,new Comparator<PollData>() {
				public int compare(PollData o1, PollData o2) {
					int idc = o1.getComparableId().compareTo(o2.getComparableId());
					if(idc==0)
						return o1.getTime().compareTo(o2.getTime());
					else
						return idc;
				}
				
			});
			LevelDBPartition instance = ring.getPartiotion(partition);
			PollDataDBTask task = new StorePollDataTask(datas);
			instance.execute(task, timeout, unit);
			Promise promise = task.getResult();
			Object result = promise.getResult(timeout, unit);
			if(!promise.isDelivered()){
				throw new PollDataDBException("Timeout write to partition "+partition);
			}
			else if(result instanceof StoreResults){
				return (StoreResults)result;
			}
			else if(result instanceof Throwable){
				throw new PollDataDBException("cant write to partition "+partition,(Throwable)result);
			}
			else{
				throw new PollDataDBException("Bad write result to partition "+partition);
			}
		} catch (InterruptedException e) {
			throw new PollDataDBException("Write to partition interrupted"+partition,e);
		}
	}
	public List<? extends PollData>  read(PollData from,PollData to,long timeout,TimeUnit unit) throws PollDataDBException{
		if(isClosed)
			throw new PollDataDBException("DB is closed");
		long fromTime = Math.max(ring.getMinTime(),from.getTime());
		long toTime = Math.min(ring.getMaxTime(),to.getTime());
		long fromPartition = ring.partition(fromTime);
		long toPartiotion = ring.partition(toTime);
		List<? extends PollData> results = new ArrayList<PollData>();
		for (long i = fromPartition; i <= toPartiotion; i++) {
			LevelDBPartition instance = ring.getPartiotion(i);
			if (instance != null) {
				Object res = null;
				PollDataDBTask task = new ScanPollDataTask();
				try {
					instance.execute(task, timeout, unit);
					res = task.getResult().getResult(timeout, unit);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new PollDataDBException("Can't scan partiotion " + i,
							e);
				}
				if (res instanceof ScanPollDataReader) {
					ScanPollDataReader scan = (ScanPollDataReader) res;
					scan.read(from, to, results);
				} else if (res instanceof Throwable) {
					throw new PollDataDBException("Can't scan partiotion " + i,
							(Throwable) res);
				} else
					throw new PollDataDBException("Can't scan partiotion " + i
							+ " uncknown result");
			}
		}
		return results;
		
	}
	public void close(){
		isClosed = true;
		ring.close();
	}

	public List<SimpleStats>  eventsStat(int type,long id,long from,long to,long timeout,TimeUnit unit) throws PollDataDBException{
		return new ArrayList<SimpleStats>();
	}
}
