package com.satissoft.mon.polldb;

import java.util.Properties;

public class PollDataDBFactory {
	private static PollDataDB  db = null ;
	public static PollDataDB getFactory(){
		synchronized(PollDataDBFactory.class){
			return db;
		}
	}
	public static void init(Class clazz,Properties properties){
		synchronized(PollDataDBFactory.class){
			if(clazz.getName().equals(LiteTsPollDataDB.class.getName())){
				db = new LiteTsPollDataDB(properties);
			}
		}
	}
}
