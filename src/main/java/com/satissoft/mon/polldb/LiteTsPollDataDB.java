package com.satissoft.mon.polldb;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class LiteTsPollDataDB implements PollDataDB {
	private static Logger log = LoggerFactory.getLogger(LiteTsPollDataDB.class);
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
	private class PolledConnection{
		RoundRobinPool pool = null;
		ConnectionWrapper w = null;
		
		public PolledConnection(RoundRobinPool pool, ConnectionWrapper w) {
			super();
			this.pool = pool;
			this.w = w;
		}

		LiteTsPollDBSockConnection connection(){
			return w.conn;
		}
		public void release(){
			pool.release(w);
		}
		public void destroy(){
			pool.destroy(w);
		}
	}
	
	private PolledConnection getNextPolledConnection() throws PollDataDBException{
		for(int i = 0 ; i < pool.length ; i++){
			RoundRobinPool pool = getPoll();
			ConnectionWrapper w = pool.getConnection();
			if(w!=null){
				return new PolledConnection(pool,w);
			}
		}
		log.error("No database instance available");
		throw new PollDataDBException("No database instance available");
	}
	public StoreResults stote(List<? extends PollData> data, long timeout,
			TimeUnit unit) {
		if(isClosed){
			return makeErrorResult(data.size(),new PollDataDBException("DB is closed!"));
		}
		PolledConnection conn;
		try {
			conn = getNextPolledConnection();
		} catch (PollDataDBException e1) {
			return makeErrorResult(data.size(),e1);
		}
		try {
			 StoreResults res = conn.connection().write(data, timeout, unit);
			 conn.release();
			 return res;
		} catch (Exception e) {
			conn.destroy();
			return makeErrorResult(data.size(),e);
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
		PolledConnection conn;
		try {
			conn = getNextPolledConnection();
		} catch (PollDataDBException e1) {
			throw e1;
		}
		try {
			List<? extends PollData> res =   conn.connection().read(from, to, timeout, unit);
			conn.release();
			return res;
		} catch (Exception e) {
			conn.destroy();
			throw new PollDataDBException("Runtime error",e);
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
		private long unvailableScince = 0;
		public RoundRobinPool(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}
		public ConnectionWrapper getConnection(){
			if((System.currentTimeMillis()-unvailableScince)<30000){
				return null;
			}
			synchronized (this) {
				if(isClosed){
					return null;
				}
				if(count>=concurentCounnections){
					log.error("No availbale connections to "+host+":"+port);
					return null;
				}
				else{
					ConnectionWrapper conn = conenctions.poll();
					if(conn==null){
						try {
							conn = new ConnectionWrapper(host, port);
						} catch (IOException e) {
							e.printStackTrace();
							log.error("Can't connect to "+host+":"+port);
							unvailableScince = System.currentTimeMillis();
							return null;
						}
					}
					else if((System.currentTimeMillis()-conn.lastAccsessTime)>liveTime){
						conn.conn.close();
						try {
							conn = new ConnectionWrapper(host, port);
						} catch (IOException e) {
							e.printStackTrace();
							unvailableScince = System.currentTimeMillis();
							log.error("Can't connect to "+host+":"+port);
							return null;
						}
					}
					unvailableScince = 0;
					conn.lastAccsessTime = System.currentTimeMillis();
					count++;
					return conn;
				}
			}
		}
		public void destroy(ConnectionWrapper conn){
			synchronized (this){
				count--;
			}
			conn.conn.close();
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
