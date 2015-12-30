package net.whitbeck.rdb_parser;

import java.util.List;

public abstract class KeyValuePair extends Entry {

  public static final int
    VALUE = 0,
    LIST = 1,
    SET = 2,
    SORTED_SET = 3,
    HASH = 4,
    ZIPMAP = 9,
    ZIPLIST = 10,
    INTSET = 11,
    SORTED_SET_AS_ZIPLIST = 12,
    HASHMAP_AS_ZIPLIST = 13;

  private byte[] ts;
  private long expiry;
  private final boolean hasExpiry;
  private final byte[] key;
  private final int valueType;

  protected KeyValuePair(int valueType, byte [] ts, byte[] key) {
    this.valueType = valueType;
    this.ts = ts;
    this.key = key;
    this.hasExpiry = ts != null;
  }

  public int getValueType() {
    return valueType;
  }

  public int getType() {
    return Entry.KEY_VALUE_PAIR;
  }

  public boolean hasExpiry() {
    return hasExpiry;
  }

  public long getExpiryMillis() {
    if (!hasExpiry) {
      throw new IllegalStateException("Entry does not have an expiry");
    }
    if (ts != null) {
      switch (ts.length) {
      case 4:
        expiry = parseExpiry4Bytes(); break;
      case 8:
        expiry = parseExpiry8Bytes(); break;
      default:
        throw new IllegalStateException("Invalid number of timestamp bytes");
      }
      ts = null;
    }
    return expiry;
  }

  private long parseExpiry4Bytes() {
    return 1000L * ((((long)ts[3] & 0xff) << 24) |
                    (((long)ts[2] & 0xff) << 16) |
                    (((long)ts[1] & 0xff) <<  8) |
                    (((long)ts[0] & 0xff) <<  0));
  }

  private long parseExpiry8Bytes() {
    return ((((long)ts[7] & 0xff) << 56) |
            (((long)ts[6] & 0xff) << 48) |
            (((long)ts[5] & 0xff) << 40) |
            (((long)ts[4] & 0xff) << 32) |
            (((long)ts[3] & 0xff) << 24) |
            (((long)ts[2] & 0xff) << 16) |
            (((long)ts[1] & 0xff) <<  8) |
            (((long)ts[0] & 0xff) <<  0));
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    throw new UnsupportedOperationException();
  }

  public List<byte[]> getValues() {
    throw new UnsupportedOperationException();
  }
}
