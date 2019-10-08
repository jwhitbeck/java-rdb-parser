/**
 * Copyright (c) 2015-2019 John Whitbeck. All rights reserved.
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
 * <p>IDLE entries contain the LRU idel time.
 *
 * @author John Whitbeck
 */
public final class Idle implements Entry {

  private final long time;

  Idle(long time) {
    this.time = time;
  }

  @Override
  public EntryType getType() {
    return EntryType.IDLE;
  }

  /**
   * @return the LRU idle time
   */
  public long getTime() {
    return time;
  }

  @Override
  public String toString() {
    return EntryType.IDLE + " (" + time + ")";
  }
}
