package com.satissoft.mon.polldb;

public abstract class GeneralPollDataDBTask implements PollDataDBTask {
	
	protected Promise result;
	
	public GeneralPollDataDBTask(){
		result = new Promise();
	}
	public Promise getResult() {
		return result;
	}
	
	public void commit(Object result){
		this.result.delivery(result);
	}

}
