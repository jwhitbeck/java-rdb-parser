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

/**
 * Key/value pair entries contain all the data associated with a given key:
 *
 * <ul>
 *  <li>The key itself</li>
 *  <li>Optionally, the expiry timestamp</li>
 *  <li>The values associated with the key</li>
 * </ul>
 *
 * @author John Whitbeck
 */
public final class KeyValuePair extends Entry {

  /**
   * A simple redis key/value pair as created by <code>set foo bar</code>.
   */
  public static final int VALUE = 0;

  /**
   * A redis list as created by <code>lpush foo bar</code>.
   */
  public static final int LIST = 1;

  /**
   *A redis set as created by <code>sadd foo bar</code>.
   */
  public static final int SET = 2;

  /**
   * A redis sorted set as created by <code>zadd foo 1.2 bar</code>.
   */
  public static final int SORTED_SET = 3;

  /**
   * A redis hash as created by <code>hset foo bar baz</code>.
   */
  public static final int HASH = 4;

  /**
   * A compact encoding for small hashes. Deprecated as of redis 2.6 and not currently supported.
   */
  public static final int ZIPMAP = 9;

  /**
   * A compact encoding for small lists.
   */
  public static final int ZIPLIST = 10;


  /**
   * A compact encoding for sets comprised entirely of integers.
   */
  public static final int INTSET = 11;


  /**
   * A compact encoding for small sorted sets in which value/score pairs are flattened and stored in a ZipList.
   */
  public static final int SORTED_SET_AS_ZIPLIST = 12;

  /**
   * A compact encoding for small hashes in which key/value pairs are flattened and stored in a ZipList
   */
  public static final int HASHMAP_AS_ZIPLIST = 13;


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


  /**
   * Returns the value type encoding.
   *
   * @return the value type encoding.
   */
  public int getValueType() {
    return valueType;
  }

  @Override
  public int getType() {
    return Entry.KEY_VALUE_PAIR;
  }

  /**
   * Returns true if this key/value pair as an expiry (either in seconds or milliseconds) associated with it,
   * false otherwise.
   *
   * @return whether or not this object has an expiry.
   */
  public boolean hasExpiry() {
    return hasExpiry;
  }

  /**
   * Returns the expiry in milliseconds. If the initial expiry was set in seconds in redis, the expiry is
   * converted to milliseconds. Throws an IllegalStateException if not expiry is present.
   *
   * @return the expiry in milliseconds.
   */
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


  /**
   * Returns the key associated with this key/value pair.
   *
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }

  /**
   * Returns the list of values (as byte-arrays) associated with this key/value pair.
   *
   * This values in this list depend on the value type.
   *
   * <ul>
   *  <li>VALUE: A singleton with the value.</li>
   *  <li>LIST, ZIPLIST, SET, INTSET: the values in the order they appear in the RDB file.</li>
   *  <li>HASH, HASHMAP_AS_ZIPLIST: a flattened list of key/value pairs.</li>
   *  <li>SORTED_SET, SORTED_SET_AS_ZIPLIST: a flattened list of key/score pairs;
   *      the scores can be parsed using {@link Double.parseDouble}.</li>
   * </ul>
   *
   * @return the list of values.
   */
  public List<byte[]> getValues() {
    if (values == null) {
      values = lazyList.get();
      lazyList = null;
    }
    return values;
  }
}
