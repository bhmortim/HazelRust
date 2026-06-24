package com.hazelcast.test;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import java.io.IOException;
import java.util.Map;
public class IncrementEntryProcessor
    implements EntryProcessor<String,String,String>, IdentifiedDataSerializable {
  private long increment;
  public IncrementEntryProcessor() {}
  @Override
  public String process(Map.Entry<String,String> entry) {
    long cur = (entry.getValue() == null) ? 0L : Long.parseLong(entry.getValue());
    long now = cur + increment;
    String s = Long.toString(now);
    entry.setValue(s);
    return s;
  }
  @Override public int getFactoryId() { return 1; }
  @Override public int getClassId() { return 1; }
  @Override public void writeData(ObjectDataOutput out) throws IOException { out.writeLong(increment); }
  @Override public void readData(ObjectDataInput in) throws IOException { increment = in.readLong(); }
}
