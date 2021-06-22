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
 * <p>End-of-file entry. This is always the last entry in the file and, as of RDB version 6,
 * contains an 8 byte checksum of the file.
 *
 * @author John Whitbeck
 */
public final class Eof implements Entry {

  private final byte[] checksum;

  Eof(byte[] checksum) {
    this.checksum = checksum;
  }

  @Override
  public EntryType getType() {
    return EntryType.EOF;
  }

  /**
   * Returns the 8-byte checksum of the rdb file. These 8 bytes are all zero if the version of RDB
   * file is 4 or older.
   *
   * @return the 8-byte checksum of the rdb file.
   */
  public byte[] getChecksum() {
    return checksum;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(EntryType.EOF);
    sb.append(" (");
    for (byte b : checksum) {
      sb.append(String.format("%02x", (int)b & 0xff));
    }
    sb.append(")");
    return sb.toString();
  }
}
