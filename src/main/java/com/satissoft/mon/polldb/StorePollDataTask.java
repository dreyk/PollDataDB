package com.satissoft.mon.polldb;

import java.util.List;

public class StorePollDataTask extends GeneralPollDataDBTask {
	List<? extends LevelDBKV>  data;
	public StorePollDataTask(List<? extends LevelDBKV> data){
		super();
		this.data = data;
	}
	public List<? extends LevelDBKV> getData(){
		return data;
	}
	
}
