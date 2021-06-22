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
 * <p>An auxiliary field contains a key value pair that holds metadata about the RDB file.
 *
 * <p>Introduced in RDB version 7.
 *
 * @author John Whitbeck
 */
public final class AuxField implements Entry {

  private final byte[] key;
  private final byte[] value;

  AuxField(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public EntryType getType() {
    return EntryType.AUX_FIELD;
  }

  /**
   * Returns the key.
   *
   * @return key
   */
  public byte[] getKey() {
    return key;
  }

  /**
   * Returns the value.
   *
   * @return value
   */
  public byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s (k: %s, v: %s)",
                         EntryType.AUX_FIELD,
                         StringUtils.getPrintableString(key),
                         StringUtils.getPrintableString(value));
  }
}
