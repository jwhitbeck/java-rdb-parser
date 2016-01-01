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

/**
 * The base class for Redis entries.
 *
 * @author John Whitbeck
 */
public abstract class Entry {

  /**
   * Denotes an end-of-file entry with a checksum.
   *
   * @see Eof
   */
  public static final int EOF = 0;

  /**
   * Denotes a DB selection entry.
   *
   * @see DbSelect
   */
  public static final int DB_SELECT = 1;

  /**
   * Denotes a key/value pair entry.
   *
   * @see KeyValuePair
   */
  public static final int KEY_VALUE_PAIR = 2;

  /**
   * Returns the entry type.
   *
   * @return one of EOF, DB_SELECT, or KEY_VALUE_PAIR
   */
  public abstract int getType();
}
