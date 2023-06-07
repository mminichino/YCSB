package site.ycsb.workloads;

import site.ycsb.ByteIterator;

import java.util.HashMap;

/**
 * Container class for generated document.
 */
public class DocumentData {

  private final int dataSize;
  private final HashMap<String, ByteIterator> dataMap;

  public DocumentData(int size, HashMap<String, ByteIterator> map) {
    this.dataSize = size;
    this.dataMap = map;
  }

  public int getSize() {
    return this.dataSize;
  }

  public HashMap<String, ByteIterator> getMap() {
    return this.dataMap;
  }

}
