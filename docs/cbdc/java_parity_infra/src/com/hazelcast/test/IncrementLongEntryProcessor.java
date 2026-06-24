package com.hazelcast.test;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import java.io.IOException;
import java.util.Map;
public class IncrementLongEntryProcessor
    implements EntryProcessor<String,Long,Long>, IdentifiedDataSerializable {
  private long increment;
  public IncrementLongEntryProcessor() {}
  @Override
  public Long process(Map.Entry<String,Long> entry) {
    long cur = (entry.getValue() == null) ? 0L : entry.getValue();
    long now = cur + increment;
    entry.setValue(now);
    return now;
  }
  @Override public int getFactoryId() { return 1; }
  @Override public int getClassId() { return 2; }
  @Override public void writeData(ObjectDataOutput out) throws IOException { out.writeLong(increment); }
  @Override public void readData(ObjectDataInput in) throws IOException { increment = in.readLong(); }
}
