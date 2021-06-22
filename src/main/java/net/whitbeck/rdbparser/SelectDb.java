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
 * <p>DB selection entries mark the beginning of a new database in the RDB dump file. All subsequent
 * {@link KeyValuePair}s until the next {@link SelectDb} or {@link Eof} entry belong to this
 * database.
 *
 * @author John Whitbeck
 */
public final class SelectDb implements Entry {

  private final long id;

  SelectDb(long id) {
    this.id = id;
  }

  @Override
  public EntryType getType() {
    return EntryType.SELECT_DB;
  }

  /**
   * Returns the identifier of this database.
   *
   * @return the database identifier
   */
  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return EntryType.SELECT_DB + " (" + id + ")";
  }
}
