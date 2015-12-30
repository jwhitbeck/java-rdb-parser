package net.whitbeck.rdb;

import java.util.List;

class KeyValues extends KeyValuePair {

  private List<byte[]> values;
  private LazyList lazyList;

  KeyValues(int valueType, byte[] ts, byte[] key, List<byte[]> values) {
    super(valueType, ts, key);
    this.values = values;
  }

  KeyValues(int valueType, byte[] ts, byte[] key, LazyList lazyList) {
    super(valueType, ts, key);
    this.lazyList = lazyList;
  }

  @Override
  public List<byte[]> getValues() {
    if (values == null) {
      values = lazyList.get();
      lazyList = null;
    }
    return values;
  }

}
