package net.whitbeck.rdb;

class KeyValues extends KeyValuePair {

  private byte[][] values;
  private LazyList lazyList;

  KeyValues(int valueType, byte[] ts, byte[] key, byte[][] values) {
    super(valueType, ts, key);
    this.values = values;
  }

  KeyValues(int valueType, byte[] ts, byte[] key, LazyList lazyList) {
    super(valueType, ts, key);
    this.lazyList = lazyList;
  }

  @Override
  public byte[][] getValues() {
    if (values == null) {
      values = lazyList.get();
      lazyList = null;
    }
    return values;
  }

}
