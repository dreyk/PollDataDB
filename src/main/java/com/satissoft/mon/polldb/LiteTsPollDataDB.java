package com.satissoft.mon.polldb;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class LiteTsPollDataDB implements PollDataDB {

	private int cursor = 0;
	private RoundRobinPool pool[];
	private int concurentCounnections;
	private boolean isClosed = false;
	private long liveTime = 60l*1000l;
	private Timer timer;
	
	public LiteTsPollDataDB(Properties conf){
		String connString = conf.getProperty("connection");
		String array[] = connString.split(",");
		String hosts[] = new String[array.length];
		int ports[] = new int[array.length];
		for(int i = 0 ; i < array.length ; i++){
			String s[] = array[i].split(":");
			hosts[i] = s[0];
			ports[i] = Integer.parseInt(s[1]);
		}
		int concurentConnctionCount = 100;
		try{
			concurentConnctionCount = Integer.parseInt(conf.getProperty("connections_per_host"));
		}
		catch (Exception e) {
		}
		init(hosts,ports,concurentConnctionCount);
		
	}
	public LiteTsPollDataDB(String hosts[],int ports[],int concurentConnctionCount){
		init(hosts,ports,concurentConnctionCount);
	}
	private void init(String hosts[],int ports[],int concurentConnctionCount){
		pool = new RoundRobinPool[hosts.length];
		this.concurentCounnections = concurentConnctionCount;
		for(int i = 0 ; i < hosts.length ; i++){
			pool[i] = new RoundRobinPool(hosts[i], ports[i]);
		}
		timer = new Timer("LiteTsPollDataDB control");
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				if(!isClosed){
					for(RoundRobinPool p:pool){
						p.closeExpired();
					}
				}
				
			}
		},liveTime, liveTime);
	}
	private RoundRobinPool getPoll(){
		int index;
		synchronized (this) {
			cursor++;
			if(cursor>=pool.length){
				cursor = 0;
			}
			index = cursor;
		}
		return pool[index];
	}
	public StoreResults stote(List<? extends PollData> data, long timeout,
			TimeUnit unit) {
		if(isClosed){
			return makeErrorResult(data.size(),new PollDataDBException("DB is closed!"));
		}
		RoundRobinPool p = getPoll();
		ConnectionWrapper w;
		try {
			w = p.getConnection();
		} catch (IOException e) {
			e.printStackTrace();
			return makeErrorResult(data.size(),new PollDataDBException("Can't connect to db",e));
		}
		if(w==null){
			return makeErrorResult(data.size(),new PollDataDBException("No available connections!"));
		}
		try {
			return w.conn.write(data, timeout, unit);
		} catch (Exception e) {
			return makeErrorResult(data.size(),e);
		}
		finally{
			p.release(w);
		}
	}
    private StoreResults makeErrorResult(int size,Throwable error){
    	StoreResults res = new StoreResults();
    	res.addError(size, error);
    	return res;
    }
	public List<? extends PollData> read(PollData from, PollData to,
			long timeout, TimeUnit unit) throws PollDataDBException {
		if(isClosed){
			throw new PollDataDBException("DB is closed!");
		}
		RoundRobinPool p = getPoll();
		ConnectionWrapper w;
		try {
			w = p.getConnection();
		} catch (IOException e) {
			e.printStackTrace();
			throw new PollDataDBException("Can't connect to db",e);
		}
		if(w==null){
			throw new PollDataDBException("No available connections!");
		}
		try {
			return w.conn.read(from, to, timeout, unit);
		} catch (Exception e) {
			throw new PollDataDBException("Runtime error",e);
		}
		finally{
			p.release(w);
		}
	}

	public void close() {
		timer.cancel();
		isClosed = true;
		for(RoundRobinPool p:pool){
			p.close();
		}
		
	}
	private class ConnectionWrapper{
		LiteTsPollDBSockConnection conn;
		long lastAccsessTime;
		public ConnectionWrapper(String host,int port) throws IOException{
			conn = new LiteTsPollDBSockConnection(host, port);
			lastAccsessTime = System.currentTimeMillis();
		}
	}
	private class RoundRobinPool{
		String host;
		int port;
		LinkedList<ConnectionWrapper> conenctions = new LinkedList<ConnectionWrapper>();
		int count = 0;
		boolean isClosed = false;
		public RoundRobinPool(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}
		public ConnectionWrapper getConnection() throws IOException{
			synchronized (this) {
				if(isClosed){
					return null;
				}
				if(count>=concurentCounnections){
					return null;
				}
				else{
					ConnectionWrapper conn = conenctions.poll();
					if(conn==null){
						conn = new ConnectionWrapper(host, port);
					}
					conn.lastAccsessTime = System.currentTimeMillis();
					count++;
					return conn;
				}
			}
		}
		public void release(ConnectionWrapper conn){
			synchronized (this){
				conn.lastAccsessTime = System.currentTimeMillis();
				if(isClosed){
					conn.conn.close();
				}
				else{
					conenctions.add(conn);
				}
				count--;
			}
		}
		public void close(){
			synchronized (this) {
				isClosed = true;
				for(ConnectionWrapper w:conenctions){
					w.conn.close();
				}
				conenctions.clear();
			}
		}
		public void closeExpired(){
			synchronized (this) {
				Iterator<ConnectionWrapper>it = conenctions.iterator();
				while(it.hasNext()){
					ConnectionWrapper w = it.next();
					if((System.currentTimeMillis()-w.lastAccsessTime)>liveTime){
						it.remove();
					}
				}
			}
		}
	}

}
