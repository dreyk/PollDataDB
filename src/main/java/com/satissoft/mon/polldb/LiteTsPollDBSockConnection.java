package com.satissoft.mon.polldb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiteTsPollDBSockConnection{
	private static Logger log = LoggerFactory.getLogger(LiteTsPollDBSockConnection.class);
	private final static byte STORE_REQ = 1;
	private final static byte SCAN_REQ= 2;
	private final static byte PING_REQ= 3;
	private final static byte EVENTS_STAT_REQ= 4;
	private final static byte EVENTS_REQ= 5;


	private final static byte OK_RESP = 0;
	private final static byte UNKNOWN_REQ_RESP = 1;
	private final static byte UNKNOWN_DATA_RESP = 2;
	private final static byte RUNTIME_ERROR_RESP = 100;
	
	private final static long pingTime = 5000l;
	
	private Socket sock;
	private DataOutputStream dout;
	private DataInputStream din;
	private boolean isStarted;
	private String host;
	private int port;
	//Timer pingTimer;
	public LiteTsPollDBSockConnection(String host,int port) throws IOException{
		this.host = host;
		this.port = port;
		sock = new Socket(host, port);
		dout = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream(), 1024 * 16));
		din = new DataInputStream(new BufferedInputStream(sock.getInputStream(), 1024 * 16));
		//pingTimer = new Timer("litetsdbconnection-"+host+":"+port);
		//pingTimer.schedule(this,pingTime,pingTime);
		isStarted = true;
		
	}
	public void stop(){
		isStarted = false;
		//pingTimer.cancel();
		close();
	}
	/*@Override
	public void run() {
		synchronized (this) {
			dout.writeInt(1);
			dout.writeByte(PING_REQ);
			dout.flush();
			if(din.readInt()!=1){
		
			}
			if(din.readInt()!=OK_RESP){
				
			}
		}
	}*/
	public void close(){
		try {
			sock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public  StoreResults write(List<? extends PollData> datas,long timeout,TimeUnit unit) throws PollDataDBException{
		int batchSize = 1;
		for(PollData d:datas){
			batchSize+=(d.getLevelDBValue().length+20);
		}
		try {
			dout.writeInt(batchSize);
			dout.write(STORE_REQ);
			for(PollData d:datas){
				byte value[] = d.getLevelDBValue();
				dout.writeInt(value.length);
				dout.writeLong((Long)d.getComparableId());
				dout.writeLong(d.getTime());
				dout.write(value);
			}
			dout.flush();
			
			//skip first 4 bytes
			din.readInt();
			int respSize = din.readInt();
			if(respSize<1){
				exitAndThrow("Bad response");
			}
			byte code = din.readByte();
			switch (code) {
			case OK_RESP:
				return readStoreResults(respSize-1);
			case RUNTIME_ERROR_RESP:
				exitOnRuntimeError(respSize-1);
				break;
			default:
				exitAndThrow("UNKNOWN response code "+code);
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
			exitAndThrow("Can't write/read data to",e);
		}
		return null;
	}
	private StoreResults readStoreResults(int size) throws PollDataDBException,IOException{
		if(size<64){
			exitAndThrow("Bad response size - "+size);
		}
		int count = din.readInt();
		int error = din.readInt();
		StoreResults res = new StoreResults();
		res.addCount(count);
		res.addErrorCount(error);
		if(size>64){
			byte message[] = new byte[size-64];
			din.readFully(message);
			res.addError(new PollDataDBException(new String(message)));
		}
		return res;
		
	}
	public List<? extends PollData> read(PollData from, PollData to,
			long timeout, TimeUnit unit) throws PollDataDBException {
		try {
			dout.writeInt(1+8*3);
			dout.write(SCAN_REQ);
			dout.writeLong((Long)from.getComparableId());
			dout.writeLong(from.getTime());
			dout.writeLong(to.getTime());
			dout.flush();
			
			//skip first 4 bytes
			din.readInt();
			int respSize = din.readInt();
			if(respSize<1){
				exitAndThrow("Bad response");
			}
			byte code = din.readByte();
			switch (code) {
			case OK_RESP:
				return readScanResults(from,respSize-1);
			case RUNTIME_ERROR_RESP:
				exitOnRuntimeError(respSize-1);
				break;
			default:
				exitAndThrow("UNKNOWN response code "+code);
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
			exitAndThrow("Can't write/read data to",e);
		}
		return null;
	}
	private List<? extends PollData> readScanResults(PollData from,int size) throws PollDataDBException,IOException{
		int readed = 0;
		List<PollData>res = new ArrayList<PollData>();
		while(readed<size){
			int dataSize = din.readInt();
			byte key[] = new byte[16];
			din.readFully(key);
			byte body[] = new byte[dataSize];
			if(dataSize>0){
				din.readFully(body);
			}
			res.add((PollData)from.initLevelDb(key, body));
			readed+=(dataSize+20);
		}
		return res;
		
	}
	public List<SimpleStats>  eventsStat(int type,long id,long from,long to,long timeout,TimeUnit unit) throws PollDataDBException {
		try {
			dout.writeInt(1+8*3+4);
			dout.write(EVENTS_STAT_REQ);
			dout.writeLong(id);
			dout.writeLong(from);
			dout.writeLong(to);
			dout.writeInt(type);
			dout.flush();
			
			//skip first 4 bytes
			din.readInt();
			int respSize = din.readInt();
			if(respSize<1){
				exitAndThrow("Bad response");
			}
			byte code = din.readByte();
			switch (code) {
			case OK_RESP:
				return  readStatResults(respSize-1);
			case RUNTIME_ERROR_RESP:
				exitOnRuntimeError(respSize-1);
				break;
			default:
				exitAndThrow("UNKNOWN response code "+code);
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
			exitAndThrow("Can't write/read data to",e);
		}
		return null;
	}
	public List<SimpleEvent>  events(long id,long from,long to,long timeout,TimeUnit unit) throws PollDataDBException{
		try {
			dout.writeInt(1+8*3);
			dout.write(EVENTS_REQ);
			dout.writeLong(id);
			dout.writeLong(from);
			dout.writeLong(to);
			dout.flush();
			
			//skip first 4 bytes
			din.readInt();
			int respSize = din.readInt();
			if(respSize<1){
				exitAndThrow("Bad response");
			}
			byte code = din.readByte();
			switch (code) {
			case OK_RESP:
				return  readEventsResults(respSize-1);
			case RUNTIME_ERROR_RESP:
				exitOnRuntimeError(respSize-1);
				break;
			default:
				exitAndThrow("UNKNOWN response code "+code);
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
			exitAndThrow("Can't write/read data to",e);
		}
		return null;
	}
	private  List<SimpleEvent> readEventsResults(int size) throws PollDataDBException,IOException{
		int readed = 0;
		List<SimpleEvent>res = new ArrayList<SimpleEvent>();
		while(readed<size){
			//<<Up:64/integer,Down:64/integer,Param:64/integer,St:32/integer,S:32/integer,IsAlarm:8/integer>>
			long up= din.readLong();
			long down= din.readLong();
			long param = din.readLong();
			int status = din.readInt();
			int severity = din.readInt();
			int isAlarm = (int)din.readByte();
			res.add(new SimpleEvent(up, down, param, severity, status, isAlarm==1?true:false));
			readed+=12;
		}
		return res;
		
	}
	private  List<SimpleStats> readStatResults(int size) throws PollDataDBException,IOException{
		int readed = 0;
		List<SimpleStats>res = new ArrayList<SimpleStats>();
		while(readed<size){
			long time = din.readLong();
			int state = din.readInt();
			res.add(new SimpleStats(time, state));
			readed+=12;
		}
		return res;
		
	}
	private void exitOnRuntimeError(int size) throws PollDataDBException,IOException{
		byte message[] = new byte[size];
		din.readFully(message);
		exitAndThrow("R untime server error",new PollDataDBException(new String(message)));
	}
	private void exitAndThrow(String message) throws PollDataDBException{
		destory();
		throw new PollDataDBException(message+" "+host+":"+port);
	}
	private void exitAndThrow(String message,Throwable reason) throws PollDataDBException{
		destory();
		throw new PollDataDBException(message+" "+host+":"+port,reason);
	}
	private void destory(){
		isStarted = false;
		try {
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
