package com.satissoft.mon.polldb;


import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.iq80.leveldb.DBIterator;

public class ScanPollDataReader {
	private DBIterator iterator;
	public ScanPollDataReader(DBIterator iterator){
		this.iterator = iterator;
	}
	public void read(PollData from,PollData to,List data){
		try {

			byte toBytes[] = to.getLevelDBKey();
			byte fromBytes[] = from.getLevelDBKey();
			iterator.seek(fromBytes);
			while(iterator.hasNext()){
				Entry<byte[],byte[]> entry = iterator.next();
				byte key[] = entry.getKey();
				if(key.length != fromBytes.length){
					return;
				}
				else{
					if(compareTo(key,fromBytes)>=0 && compareTo(key, toBytes)<=0){
						data.add((PollData)from.initLevelDb(key,entry.getValue()));
					}
					else
						return;
				}
			}
		} finally{
			try {
				iterator.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public int compareTo(byte[] buffer1,byte[] buffer2){
		return compareTo(buffer1,0,buffer1.length, buffer2,0,buffer2.length);
	}
	public int compareTo(byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
			return 0;
		}
		// Bring WritableComparator code local
		int end1 = offset1 + length1;
		int end2 = offset2 + length2;
		for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
			int a = (buffer1[i] & 0xff);
			int b = (buffer2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return length1 - length2;
	}
	    
}
