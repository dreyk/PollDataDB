package com.satissoft.mon.polldb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class LiteTsPollDBSockConnection{
	//private static Logger log = LoggerFactory.getLogger(LiteTsPollDBSockConnection.class);
	private final static byte STORE_REQ = 1;
	private final static byte SCAN_REQ= 2;

	private final static byte OK_RESP = 0;
	//private final static byte UNKNOWN_REQ_RESP = 1;
	//private final static byte UNKNOWN_DATA_RESP = 2;
	private final static byte RUNTIME_ERROR_RESP = 100;
	
	//private final static long pingTime = 5000l;
	
	private Socket sock;
	private DataOutputStream dout;
	private DataInputStream din;
	private String host;
	private int port;
	public LiteTsPollDBSockConnection(String host,int port) throws IOException{
		this.host = host;
		this.port = port;
		sock = new Socket(host, port);
		dout = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream(), 1024 * 16));
		din = new DataInputStream(new BufferedInputStream(sock.getInputStream(), 1024 * 16));
		
	}
	public void stop(){
		close();
	}
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
			batchSize+=(d.getLevelDBValue().length+d.getLevelDBKey().length+5);
		}
		try {
			dout.writeInt(batchSize);
			dout.write(STORE_REQ);
			for(PollData d:datas){
				byte value[] = d.getLevelDBValue();
				byte key[] = d.getLevelDBKey();
				dout.write((byte)key.length);
				dout.write(key);
				dout.writeInt(value.length);
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
			byte fromKey[] = from.getLevelDBKey();
			byte toKey[] = to.getLevelDBKey();
			dout.writeInt(1+fromKey.length+toKey.length+2);
			dout.write(SCAN_REQ);
			dout.write((byte)fromKey.length);
			dout.write(fromKey);
			dout.write((byte)toKey.length);
			dout.write(toKey);
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
			int keySize = (int)din.read();
			byte key[] = new byte[keySize];
			din.readFully(key);
			int dataSize = din.readInt();
			byte body[] = new byte[dataSize];
			if(dataSize>0){
				din.readFully(body);
			}
			res.add((PollData)from.initLevelDb(key, body));
			readed+=(dataSize+keySize+5);
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
		try {
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
