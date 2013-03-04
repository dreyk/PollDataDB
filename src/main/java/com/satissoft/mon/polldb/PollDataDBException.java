package com.satissoft.mon.polldb;

public class PollDataDBException extends Exception{
	public PollDataDBException(String message){
		super(message);
	}
	public PollDataDBException(String message,Throwable cause){
		super(message,cause);
	}
}
