package test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.satissoft.mon.polldb.PollDataDB;
import com.satissoft.mon.polldb.PollDataDBFactory;
import com.satissoft.mon.polldb.StoreResults;

public class ManualTests {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ManualTests test = new ManualTests();
		test.test(args[0],args[1]);

	}
	private void test(String id,String file){
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
			PollDataDB db = PollDataDBFactory.getFactory();
			List<SimplePollData> data = new ArrayList<SimplePollData>();
			long c = 0;
			long summ = 0;
			long now = System.currentTimeMillis();
			for(long day = 0 ; day < 5 ; day++){
				for(long m = 0 ; m < 24l*60l ; m+=5){
					SimplePollData d = new SimplePollData(id,now-(day*24l*60l+m)*60l*1000l,Long.toString(c));
					data.add(d);
					summ=+c;
					c++;
					
				}
			}
			StoreResults res = db.stote(data,1,TimeUnit.MINUTES);
			System.out.println("count "+res.getCount()+" error "+res.getErrorCount());
			long start= System.currentTimeMillis();
			List<SimplePollData> poll = (List<SimplePollData>)db.read(new SimplePollData(id,now-5*24l*60l*60l*1000l,Long.toString(c)),new SimplePollData(id,now,Long.toString(c)), 1,TimeUnit.MINUTES);
			for(int i = 0 ; i < poll.size() ; i++){
				String sc = poll.get(i).getValue();
				summ -= Long.parseLong(sc);
			}
			System.out.println("check "+summ+" time "+(System.currentTimeMillis()-start));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
