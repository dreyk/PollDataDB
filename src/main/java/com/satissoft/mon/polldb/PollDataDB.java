package com.satissoft.mon.polldb;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface PollDataDB {
	public StoreResults stote(List<? extends PollData>  data,long timeout,TimeUnit unit);
	public List<? extends PollData>  read(PollData from,PollData to,long timeout,TimeUnit unit) throws PollDataDBException;
	public List<SimpleStats>  eventsStat(int type,long id,long from,long to,long timeout,TimeUnit unit) throws PollDataDBException;
	public List<SimpleEvent>  events(long id,long from,long to,long timeout,TimeUnit unit) throws PollDataDBException;
	public void close();
}
