package com.satissoft.mon.polldb;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Promise {
	Object result = null;
	private CountDownLatch lock;
	public Promise(){
		lock = new CountDownLatch(1);
	}
	public Object getResult(long timeout,TimeUnit unit) throws InterruptedException{
		lock.await(timeout, unit);
		return result;
	}
	public void delivery(Object result){
		this.result = result;
		lock.countDown();
	}
}
