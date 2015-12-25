package net.whitbeck.rdb;

class KeyValue extends KeyValuePair {

  private final byte[] str;

  KeyValue(byte[] ts, byte[] key, byte[] str) {
    super(VALUE, ts, key);
    this.str = str;
  }

  @Override
  public byte[] getValue() {
    return str;
  }

}
