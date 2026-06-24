package com.hazelcast.test;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
public class JavaParityFactory implements DataSerializableFactory {
  public static final int FACTORY_ID = 1;
  @Override
  public IdentifiedDataSerializable create(int classId) {
    switch (classId) {
      case 1: return new IncrementEntryProcessor();
      case 2: return new IncrementLongEntryProcessor();
      default: return null;
    }
  }
}
