package com.satissoft.mon.polldb;

@SuppressWarnings("serial")
public class PollDataDBException extends Exception{
	public PollDataDBException(String message){
		super(message);
	}
	public PollDataDBException(String message,Throwable cause){
		super(message,cause);
	}
}
