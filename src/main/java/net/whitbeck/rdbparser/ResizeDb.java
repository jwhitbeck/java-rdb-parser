/**
 * Copyright (c) 2015-2021 John Whitbeck. All rights reserved.
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

/**
 * <p>Resize DB entries contain information to speed up RDB loading by avoiding additional resizes
 * and rehashing.
 *
 * <p>Specifically, it contains the following:
 * <ul>
 *   <li>database hash table size; and</li>
 *   <li>expire time hash table size.</li>
 * </ul>
 *
 * <p>Introduced in RDB version 7.
 *
 * @author John Whitbeck
 */
public final class ResizeDb implements Entry {

  private final long dbHashTableSize;
  private final long expireTimeHashTableSize;

  ResizeDb(long dbHashTableSize, long expireTimeHashTableSize) {
    this.dbHashTableSize = dbHashTableSize;
    this.expireTimeHashTableSize = expireTimeHashTableSize;
  }

  @Override
  public EntryType getType() {
    return EntryType.RESIZE_DB;
  }

  /**
   * Returns the size of the DB hash table.
   *
   * @return size of the DB hash table.
   */
  public long getDbHashTableSize() {
    return dbHashTableSize;
  }

  /**
   * Returns the size of the expire time hash table.
   *
   * @return size of the expire time hash table.
   */
  public long getExpireTimeHashTableSize() {
    return expireTimeHashTableSize;
  }

  @Override
  public String toString() {
    return String.format("%s (db hash table size: %d, expire time hash table size: %d)",
                         EntryType.RESIZE_DB, dbHashTableSize, expireTimeHashTableSize);
  }
}
