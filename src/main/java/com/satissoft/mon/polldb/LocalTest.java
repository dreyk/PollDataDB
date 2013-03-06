package com.satissoft.mon.polldb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class LocalTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LocalTest test = new LocalTest();
		test.init();
		test.test();
		
		

	}
	public void read(){
		SimplePollData from = new SimplePollData(0l,System.currentTimeMillis()-1000l*60l*60l,0,"");
		SimplePollData to = new SimplePollData(0l,System.currentTimeMillis(),0,"");
		try {
			List<SimplePollData> l = (List<SimplePollData>)PollDataDBFactory.getFactory().read(from, to,1,TimeUnit.MINUTES);
			for(SimplePollData d:l){
				System.out.println("data "+d.getId()+" "+d.getTime()+" "+d.getValue());
			}
			PollDataDBFactory.getFactory().close();
		} catch (PollDataDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void init(){
		Properties p = new Properties();
		//p.
		try {
			//InputStream in  = ClassLoader.getSystemClassLoader().getResourceAsStream("sample.conf");
			//p.load(in);
			//in.close();
			p.put("clazz", "com.satissoft.mon.polldb.CasandraPollDataDB");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			PollDataDBFactory.init(Class.forName(p.getProperty("clazz")), p);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	int count = 10;
	int device = 5;
	long testime = 10;
	long batchSize = 100;
	long repInterval = 10;
	private  AtomicLong ops = new AtomicLong(0);
	private long repStart = 0;
	public void test(){
		final CountDownLatch l = new CountDownLatch(count);
		for(int i = 0 ; i < count ; i++){
			final long dev = i*device;
			Thread t = new Thread(new Runnable() {
				
				public void run() {
					write(dev);
					l.countDown();
				}
			});
			t.start();
		}
		final Timer report = new Timer();
		repStart = System.currentTimeMillis();
		report.schedule(new TimerTask() {
			
			@Override
			public void run() {
				long c = ops.getAndSet(0);
				long time = System.currentTimeMillis();
				long delta = time-repStart;
				if(delta>0){
					System.out.println("ops/s "+c*1000l/delta);
				}
				repStart = time;		
			}
		}, repInterval*1000,repInterval*1000);
		try {
			l.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		report.cancel();
		PollDataDBFactory.getFactory().close();
	}
	
	
	public void write(long id){
		List<SimplePollData> datas = new ArrayList<SimplePollData>();
		Long start = System.currentTimeMillis();
		long t = 0;
		long delta = 0;
		int count = 0;
		while((delta=(System.currentTimeMillis()-start))<testime*1000l*60l){
			SimplePollData d = new SimplePollData(t % device+id,t+start,0,null);
			datas.add(d);
			count++;
			t++;
			if(t > 1000l*60l*60l){
				start = System.currentTimeMillis();
				t = 0;
			}
			if(count==batchSize){
				try {
					StoreResults wr = PollDataDBFactory.getFactory().stote(datas,1,TimeUnit.MINUTES);
					if(wr.getCount()!=count){
						System.err.println("Write bad count! "+wr);
					}
					ops.getAndAdd(count);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				datas = new ArrayList<SimplePollData>();
				count = 0;
			}
		}
	}
	
}
