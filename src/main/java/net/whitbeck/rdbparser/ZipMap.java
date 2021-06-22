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

import java.util.ArrayList;
import java.util.List;

final class ZipMap extends LazyList<byte[]> {

  private final byte[] envelope;

  ZipMap(byte[] envelope) {
    this.envelope = envelope;
  }

  @Override
  protected List<byte[]> realize() {
    // The structure of the zip map is:
    // <zmlen><len>"foo"<len><free>"bar"<len>"hello"<len><free>"world"<zmend>

    int pos = 0;
    // The first byte holds the size of the zip map. If it is greater than or equal to 254,
    // value is not used and we will have to iterate the entire zip map to find the length.
    int zmlen = (int)envelope[pos++] & 0xff;
    List<byte[]> list = zmlen < 254 ? new ArrayList<byte[]>(2*zmlen) : new ArrayList<byte[]>();
    while (true) {
      int b = (int)envelope[pos++] & 0xff;
      if (b == 255) { // reached end of zipmap
        break;
      }
      // Read a key/value pair.

      // Read the length of the following string, which can be either a key or a value. This length
      // is stored in either 1 byte or 5 bytes. If the first byte is between 0 and 252, that is the
      // length of the value. If the first byte is 253, then the next 4 bytes read as an unsigned
      // integer represent the length of the zipmap. 254 and 255 are invalid values for this field.
      int len = 0;
      if (b < 253) {
        len = b;
      } else {
        len = ((int)envelope[pos++] & 0xff) <<  24
            | ((int)envelope[pos++] & 0xff) <<  16
            | ((int)envelope[pos++] & 0xff) << 8
            | ((int)envelope[pos++] & 0xff) << 0;
      }
      // Read the key.
      byte[] buf = new byte[len];
      System.arraycopy(envelope, pos, buf, 0, len);
      pos += len;
      list.add(buf);

      // Read the length of the value (similar to the length of the key).
      b = (int)envelope[pos++] & 0xff;
      len = 0;
      if (b < 253) {
        len = b;
      } else {
        len = ((int)envelope[pos++] & 0xff) <<  24
            | ((int)envelope[pos++] & 0xff) <<  16
            | ((int)envelope[pos++] & 0xff) << 8
            | ((int)envelope[pos++] & 0xff) << 0;
      }
      // Read the number of free bytes after the value. This is always 1 byte.For example, if the
      // value of a key is “America” and its get updated to “USA”, 4 free bytes will be available.
      int free = (int)envelope[pos++] & 0xff;
      // Read the value.
      buf = new byte[len];
      System.arraycopy(envelope, pos, buf, 0, len);
      pos += len + free;
      list.add(buf);
    }
    return list;
  }
}
