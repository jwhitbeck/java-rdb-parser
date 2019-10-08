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
 * <p>FREQ entries contain the LFU frequency.
 *
 * @author John Whitbeck
 */
public final class Freq implements Entry {

  private final int freq;

  Freq(int freq) {
    this.freq = freq;
  }

  @Override
  public EntryType getType() {
    return EntryType.FREQ;
  }

  /**
   * @return the LFU frequency
   */
  public int getFrequency() {
    return freq;
  }

  @Override
  public String toString() {
    return EntryType.FREQ + " (" + freq + ")";
  }
}
