/**
 * Copyright (c) 2015-2018 John Whitbeck. All rights reserved.
 *
 * <p>The use and distribution terms for this software are covered by the
 * Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0.txt)
 * which can be found in the file al-v20.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * <p>You must not remove this notice, or any other, from this software.
 */

package net.whitbeck.rdbparser;

import java.util.List;

/**
 * Key/value pair entries contain all the data associated with a given key. This data includes:
 *
 * <ul>
 *  <li>the key itself;</li>
 *  <li>optionally, the expiry timestamp;</li>
 *  <li>the values associated with the key.</li>
 * </ul>
 *
 * @author John Whitbeck
 */
public final class KeyValuePair implements Entry {

  private byte[] ts;
  private long expiry;
  private final boolean hasExpiry;
  private final byte[] key;
  private final ValueType valueType;
  private List<byte[]> values;
  private LazyList lazyList;

  KeyValuePair(ValueType valueType, byte [] ts, byte[] key, List<byte[]> values) {
    this.valueType = valueType;
    this.ts = ts;
    this.key = key;
    this.hasExpiry = ts != null;
    this.values = values;
  }

  KeyValuePair(ValueType valueType, byte [] ts, byte[] key, LazyList lazyList) {
    this.valueType = valueType;
    this.ts = ts;
    this.key = key;
    this.hasExpiry = ts != null;
    this.lazyList = lazyList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(EntryType.KEY_VALUE_PAIR);
    sb.append(" (key: ");
    sb.append(StringUtils.getPrintableString(key));
    if (hasExpiry()) {
      sb.append(", expiry: ");
      sb.append(getExpiryMillis());
    }
    sb.append(", ");
    int len = getValues().size();
    sb.append(len);
    if (len == 1) {
      sb.append(" value)");
    } else {
      sb.append(" values)");
    }
    return sb.toString();
  }

  /**
   * Returns the value type encoding.
   *
   * @return the value type encoding.
   */
  public ValueType getValueType() {
    return valueType;
  }

  @Override
  public EntryType getType() {
    return EntryType.KEY_VALUE_PAIR;
  }

  /**
   * Returns true if this key/value pair as an expiry (either in seconds or milliseconds) associated
   * with it, false otherwise.
   *
   * @return whether or not this object has an expiry.
   */
  public boolean hasExpiry() {
    return hasExpiry;
  }

  /**
   * Returns the expiry in milliseconds. If the initial expiry was set in seconds in redis, the
   * expiry is converted to milliseconds. Throws an IllegalStateException if not expiry is present.
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
          expiry = parseExpiry4Bytes();
          break;
        case 8:
          expiry = parseExpiry8Bytes();
          break;
        default:
          throw new IllegalStateException("Invalid number of timestamp bytes");
      }
      ts = null;
    }
    return expiry;
  }

  private long parseExpiry4Bytes() {
    return 1000L * ( ((long)ts[3] & 0xff) << 24
                   | ((long)ts[2] & 0xff) << 16
                   | ((long)ts[1] & 0xff) <<  8
                   | ((long)ts[0] & 0xff) <<  0);
  }

  private long parseExpiry8Bytes() {
    return ((long)ts[7] & 0xff) << 56
         | ((long)ts[6] & 0xff) << 48
         | ((long)ts[5] & 0xff) << 40
         | ((long)ts[4] & 0xff) << 32
         | ((long)ts[3] & 0xff) << 24
         | ((long)ts[2] & 0xff) << 16
         | ((long)ts[1] & 0xff) <<  8
         | ((long)ts[0] & 0xff) <<  0;
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
   * <p>The values in this list depend on the value type.
   *
   * <ul>
   *  <li>VALUE: A singleton with the value.</li>
   *  <li>LIST, ZIPLIST, SET, INTSET: the values in the order they appear in the RDB file.</li>
   *  <li>HASH, HASHMAP_AS_ZIPLIST: a flattened list of key/value pairs.</li>
   *  <li>SORTED_SET, SORTED_SET_AS_ZIPLIST: a flattened list of key/score pairs;
   *      the scores can be parsed using {@code Double.parseDouble}.</li>
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
