/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
