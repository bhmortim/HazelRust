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
import com.hazelcast.map.MapStore;
import java.util.*;
public class JavaParityMapStore implements MapStore<String,String> {
  public String load(String key) { return "loaded_" + key; }
  public Map<String,String> loadAll(Collection<String> keys) {
    Map<String,String> m = new HashMap<>();
    for (String k : keys) m.put(k, "loaded_" + k);
    return m;
  }
  public Iterable<String> loadAllKeys() { return Arrays.asList("load_1","load_2"); }
  public void store(String k, String v) {}
  public void storeAll(Map<String,String> m) {}
  public void delete(String k) {}
  public void deleteAll(Collection<String> keys) {}
}
