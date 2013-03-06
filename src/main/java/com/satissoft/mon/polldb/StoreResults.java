package com.satissoft.mon.polldb;

import java.util.ArrayList;
import java.util.List;

public class StoreResults {
	private int count = 0;
	private int skipCount = 0;
	private int errorCount = 0;
	public int getErrorCount() {
		return errorCount;
	}
	List<Throwable> errors = new ArrayList<Throwable>();
	public int getCount() {
		return count;
	}
	
	public int getSkipCount() {
		return skipCount;
	}
	
	public List<Throwable> getErrors() {
		return errors;
	}
	public void add(StoreResults res){
		count+=res.count;
		skipCount+=res.skipCount;
		errorCount+=res.errorCount;
		if(res.errors.size()>0){
			errors.addAll(res.errors);
		}
	}
	public void addCount(int i){
		count+=i;
	}
	public void addSkip(int i){
		skipCount+=i;
	}
	public void addError(int i,Throwable error){
		errorCount+=i;
		errors.add(error);
	}
	public void addErrorCount(int i){
		errorCount+=i;
	}
	public void addError(Throwable error){
		errors.add(error);
	}
}
