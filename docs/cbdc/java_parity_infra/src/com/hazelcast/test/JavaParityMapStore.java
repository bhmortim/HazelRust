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
