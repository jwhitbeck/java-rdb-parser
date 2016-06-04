/**
 * Copyright (c) 2015-2016 John Whitbeck. All rights reserved.
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
 * Resize DB entries contain two values to speed up RDB loading by avoiding additional resizes and
 * rehashing:
 *   - database hash table size
 *   - expiry hash table size
 *
 * Introduced in RDB version 7.
 *
 * @author John Whitbeck
 */
public final class ResizeDb implements Entry {

  private final long dbHashTableSize;
  private final long expiryHashTableSize;

  ResizeDb(long dbHashTableSize, long expiryHashTableSize) {
    this.dbHashTableSize = dbHashTableSize;
    this.expiryHashTableSize = expiryHashTableSize;
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
   * Returns the size of the expiry hash table.
   *
   * @return size of the expiry hash table.
   */
  public long getExpiryHashTableSize() {
    return expiryHashTableSize;
  }

  @Override
  public String toString() {
    return String.format("%s (db: %d, expiry: %d)",
                         EntryType.RESIZE_DB, dbHashTableSize, expiryHashTableSize);
  }
}
