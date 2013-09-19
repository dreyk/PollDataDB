package test;

import java.nio.ByteBuffer;

import com.satissoft.mon.polldb.LevelDBKV;
import com.satissoft.mon.polldb.PollData;

public class SimplePollData implements PollData{
    public LevelDBKV initLevelDb(byte[] key, byte[] data) {
        SimplePollData i = new SimplePollData();
        ByteBuffer buffer = ByteBuffer.wrap(key);
        byte kb[] = new byte[key.length-8];
        buffer.get(kb);
        i.setId(new String(kb));
        i.setTime(buffer.getLong());
        buffer = ByteBuffer.wrap(data);
        i.setValue(new String(buffer.array(),buffer.position(),buffer.remaining()));
        return i;
    }
    public byte[] getLevelDBKey() {
        byte k[] = id.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(8+k.length);
        buffer.put(k);
        buffer.putLong(time);
        return buffer.array();
    }
    public byte[] getLevelDBValue() {
        ByteBuffer buffer = ByteBuffer.allocate(value==null?0:value.length());
        if(value!=null)
            buffer.put(value.getBytes());
        return buffer.array();
    }
    String id;
    Long time;
    public SimplePollData(){
        
    }
    public SimplePollData(String id, Long time,String value) {
        super();
        this.id = id;
        this.time = time;
        this.value = value;
    }
    public Comparable getComparableId() {
        return id;
    }
    String value;
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public Long getTime() {
        return time;
    }
    public void setTime(Long time) {
        this.time = time;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
