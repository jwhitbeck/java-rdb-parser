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

// adapted from https://github.com/ganghuawang/java-redis-rdb
final class Lzf {

  // The maximum number of literals in a chunk (32).
  private static int MAX_LITERAL = 32;

  static void expand(byte[] src, byte[] dest) {
    int srcPos = 0;
    int destPos = 0;
    do {
      int ctrl = src[srcPos++] & 0xff;
      if (ctrl < MAX_LITERAL) {
        // literal run of length = ctrl + 1,
        ctrl++;
        // copy to output and move forward this many bytes
        System.arraycopy(src, srcPos, dest, destPos, ctrl);
        destPos += ctrl;
        srcPos += ctrl;
      } else {
        /* back reference
           the highest 3 bits are the match length */
        int len = ctrl >> 5;
        // if the length is maxed, add the next byte to the length
        if (len == 7) {
          len += src[srcPos++] & 0xff;
        }
        /* minimum back-reference is 3 bytes,
           so 2 was subtracted before storing size */
        len += 2;

        /* ctrl is now the offset for a back-reference...
           the logical AND operation removes the length bits */
        ctrl = -((ctrl & 0x1f) << 8) - 1;

        // the next byte augments/increases the offset
        ctrl -= src[srcPos++] & 0xff;

        /* copy the back-reference bytes from the given
           location in output to current position */
        ctrl += destPos;
        for (int i = 0; i < len; i++) {
          dest[destPos++] = dest[ctrl++];
        }
      }
    } while (destPos < dest.length);
  }

}
