/**
 * Copyright (c) 2015-2016 John Whitbeck. All rights reserved.
 *
 * The use and distribution terms for this software are covered by the
 * Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0.txt)
 * which can be found in the file al-v20.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * You must not remove this notice, or any other, from this software.
 */

package net.whitbeck.rdb_parser;

import java.util.List;

public final class KeyValuePair extends Entry {

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
  private List<byte[]> values;
  private LazyList lazyList;

  KeyValuePair(int valueType, byte [] ts, byte[] key, List<byte[]> values) {
    this.valueType = valueType;
    this.ts = ts;
    this.key = key;
    this.hasExpiry = ts != null;
    this.values = values;
  }

  KeyValuePair(int valueType, byte [] ts, byte[] key, LazyList lazyList) {
    this.valueType = valueType;
    this.ts = ts;
    this.key = key;
    this.hasExpiry = ts != null;
    this.lazyList = lazyList;
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

  public List<byte[]> getValues() {
    if (values == null) {
      values = lazyList.get();
      lazyList = null;
    }
    return values;
  }
}
