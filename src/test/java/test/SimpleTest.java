package test;

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

import com.satissoft.mon.polldb.PollDataDBException;
import com.satissoft.mon.polldb.PollDataDBFactory;
import com.satissoft.mon.polldb.StoreResults;


public class SimpleTest {
	/**
	 * @param args
	 */
	int workersCount = 10;
	int batchSize = 700;
	long readTime = 1;
	String dataPrefix = "test";
	long testime = 1;
	long repInterval = 10;
	
	public static void main(String[] args) {
		SimpleTest test = new SimpleTest();
		test.init(args[0]);
		test.test();
	}

	public void init(String file){
		Properties p = new Properties();
		try {
			InputStream in  = new FileInputStream(file);
			p.load(in);
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			PollDataDBFactory.init(Class.forName(p.getProperty("clazz")), p);
			workersCount = Integer.parseInt(p.getProperty("workers"));
			batchSize = Integer.parseInt(p.getProperty("batchSize"));
			testime = Integer.parseInt(p.getProperty("testTime"));
			dataPrefix = p.getProperty("dataPrefix");
			repInterval = Integer.parseInt(p.getProperty("reportTime"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	
	}
	private  AtomicLong opsW = new AtomicLong(0);
	private  AtomicLong opsR = new AtomicLong(0);
	private  AtomicLong opsWE = new AtomicLong(0);
	private  AtomicLong opsRE = new AtomicLong(0);
	private long repStart = 0;
	public void test(){
		final CountDownLatch bar = new CountDownLatch(workersCount);
		for(int i = 0 ; i < workersCount ; i++){
			final long dev = i;
			Thread t = new Thread(new Runnable() {
				public void run() {
					write(dev);
					bar.countDown();
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
				long we = opsWE.getAndSet(0);
				long re = opsRE.getAndSet(0);
				long time = System.currentTimeMillis();
				long delta = time-repStart;
				if(delta>0){
					System.out.println("write ops/s "+w*1000l/delta);
					System.out.println("read ops/s "+r*1000l/delta);
					System.out.println("error write ops/s "+we*1000l/delta);
					System.out.println("error read ops/s "+re*1000l/delta);
				}
				repStart = time;		
			}
		}, repInterval*1000,repInterval*1000);
		try {
			bar.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		report.cancel();
		PollDataDBFactory.getFactory().close();
	}
	
	
	public void write(long id) {
		String src = dataPrefix+id;
		long start = System.currentTimeMillis();
		long count = 0;
		long now = start;
		while((now-start)<testime*1000l*60l){
			List<SimplePollData> datas = new ArrayList<SimplePollData>(batchSize);
			do{
				SimplePollData d = new SimplePollData(src,start+count,dataPrefix+id+count);
				datas.add(d);
				count++;
				
			}while(count%batchSize!=0);
			try {
				PollDataDBFactory.getFactory().stote(datas,
						1, TimeUnit.MINUTES);
				opsW.getAndAdd(batchSize);
			} catch (PollDataDBException e) {
				opsWE.getAndAdd(batchSize);
			}
			/*try {
				List<SimplePollData> l = (List<SimplePollData>)PollDataDBFactory.getFactory().read(new SimplePollData(src,t-readTime*1000*60,null),new SimplePollData(src,t,null), 1,TimeUnit.MINUTES);
				opsR.getAndAdd(1);
			} catch (PollDataDBException e) {
				e.printStackTrace();
			}*/
		}
	}

}
