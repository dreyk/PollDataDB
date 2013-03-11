package com.satissoft.mon.polldb;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceTest {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PerformanceTest test = new PerformanceTest();
		test.init(args[0]);
		test.test();
		
		

	}

	public void init(String file){
		Properties p = new Properties();
		//p.
		try {
			InputStream in  = new FileInputStream(file);
			p.load(in);
			in.close();
			//p.put("clazz", "com.satissoft.mon.polldb.CasandraPollDataDB");
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
	int controlers = 10;
	int readOn = 8;
	int device = 700;
	long testime = 10;
	long repInterval = 10;
	private  AtomicLong opsW = new AtomicLong(0);
	private  AtomicLong opsR = new AtomicLong(0);
	private long repStart = 0;
	public void test(){
		final CountDownLatch l = new CountDownLatch(controlers);
		for(int i = 0 ; i < controlers ; i++){
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
				long w = opsW.getAndSet(0);
				long r = opsR.getAndSet(0);
				long time = System.currentTimeMillis();
				long delta = time-repStart;
				if(delta>0){
					System.out.println("write ops/s "+w*1000l/delta);
					System.out.println("read ops/s "+r*1000l/delta);
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
	
	
	public void write(long id) {
		Long start = System.currentTimeMillis();
		int count = 1;
		while ((System.currentTimeMillis() - start) < testime * 1000l * 60l) {
			if (count == readOn) {
				List<SimplePollData> l =  null;
				try {
					l = (List<SimplePollData>)PollDataDBFactory.getFactory().read(new SimplePollData(id,start,0,null),new SimplePollData(id,System.currentTimeMillis(),0,null), 1,TimeUnit.MINUTES);
					opsR.getAndAdd(l.size());
				} catch (PollDataDBException e) {
					e.printStackTrace();
				}
				if(l.size()<1){
					System.err.println("Read bad count! ");
				}
				count = 0;

			} else {
				List<SimplePollData> datas = new ArrayList<SimplePollData>();
				for (long i = id; i < id + device; i++) {
					SimplePollData d = new SimplePollData(i,
							System.currentTimeMillis(), 0, "test");
					datas.add(d);

				}
				StoreResults wr = PollDataDBFactory.getFactory().stote(datas,
						1, TimeUnit.MINUTES);
				if (wr.getCount() != device) {
					System.err.println("Write bad count! " + wr);
				}
				else{
					opsW.getAndAdd(device);
				}
				count++;
			}
		}
	}

}
