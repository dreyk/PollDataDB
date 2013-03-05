package com.satissoft.mon.polldb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;


public class CasandraPollDataDB implements PollDataDB {

	Cluster cluster;
	KeyspaceDefinition keyspaceDef;
	Keyspace ksp;
	ColumnFamilyTemplate<byte[], byte[]> template;
	long archiveDeps = 60l*60l*1000l*24l;
	public CasandraPollDataDB(){
		cluster = HFactory.getOrCreateCluster("polldb","192.168.66.34:9160");
		try {
			keyspaceDef = cluster.describeKeyspace("polldata");
		} catch (HectorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(keyspaceDef==null){
			ColumnFamilyDefinition cfDef = HFactory
					.createColumnFamilyDefinition("polldata",
							"polldata", ComparatorType.BYTESTYPE);

			keyspaceDef = HFactory.createKeyspaceDefinition(
					"polldata", ThriftKsDef.DEF_STRATEGY_CLASS,
					3, Arrays.asList(cfDef));
			cluster.addKeyspace(keyspaceDef, true);
		}
		ksp = HFactory.createKeyspace("polldata", cluster);
		template =
                new ThriftColumnFamilyTemplate<byte[], byte[]>(ksp,"polldata",BytesArraySerializer.get(),BytesArraySerializer.get());
		
	}
	public int stote(List<? extends PollData> datas, long timeout, TimeUnit unit)
			throws PollDataDBException {
		if(datas.size()<1)
			return 0;
		Collections.sort(datas,new Comparator<PollData>() {
			public int compare(PollData o1, PollData o2) {
				int idc = o1.getComparableId().compareTo(o2.getComparableId());
				if(idc==0)
					return o1.getTime().compareTo(o2.getTime());
				else
					return idc;
			}
			
		});
		PollData prev = datas.get(0);
		long prevPartition = partition(prev.getTime());
		ColumnFamilyUpdater<byte[],byte[]> updater = template.createUpdater(dk((Long)prev.getComparableId(),prevPartition));
		updater.setByteArray(cl(prev.getTime()),prev.getLevelDBValue());
		for(int i = 1 ; i < datas.size() ; i++){
			PollData d = datas.get(i);
			long partition = partition(d.getTime());
			if(d.getComparableId().compareTo(prev.getComparableId())!=0 || partition!=prevPartition){
				updater.addKey(dk((Long)d.getComparableId(),partition));
				updater.setByteArray(cl(d.getTime()),d.getLevelDBValue());
			}
			else if(prev.getTime().compareTo(d.getTime())!=0){
				updater.setByteArray(cl(d.getTime()),d.getLevelDBValue());
			}
		}
		template.executeBatch();
		return 0;
	}

	public List<? extends PollData> read(PollData from, PollData to,
			long timeout, TimeUnit unit) throws PollDataDBException {
		// TODO Auto-generated method stub
		return null;
	}

	public void close() {
		// TODO Auto-generated method stub

	}
	private byte[] dk(long id,long p){
		ByteBuffer buff = ByteBuffer.allocate(17);
		buff.put((byte)1);
		buff.putLong(id);
		buff.putLong(p);
		return buff.array();
	}
	private long partition(long time){
		return (long)(time/archiveDeps);
	}
	private byte[] cl(long time){
		ByteBuffer buff = ByteBuffer.allocate(8);
		buff.putLong(time);
		return buff.array();
	}
	
}
