package net.whitbeck.rdbparser;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.nio.charset.Charset;

class ListpackList extends LazyList<byte[]> {
  private static final Charset ASCII = Charset.forName("ASCII");

  // Taken from
  // https://github.com/redis/redis/blob/7.0.11/src/listpack.c#L55-L95C4
  private static final int LP_ENCODING_7BIT_UINT = 0;
  private static final int LP_ENCODING_7BIT_UINT_MASK = 0x80;
  private static final int LP_ENCODING_6BIT_STR = 0x80;
  private static final int LP_ENCODING_6BIT_STR_MASK = 0xC0;
  private static final int LP_ENCODING_13BIT_INT = 0xC0;
  private static final int LP_ENCODING_13BIT_INT_MASK = 0xE0;
  private static final int LP_ENCODING_12BIT_STR = 0xE0;
  private static final int LP_ENCODING_12BIT_STR_MASK = 0xF0;

  // Sub encodings
  private static final int LP_ENCODING_16BIT_INT = 0xF1;
  private static final int LP_ENCODING_16BIT_INT_MASK = 0xFF;
  private static final int LP_ENCODING_24BIT_INT = 0xF2;
  private static final int LP_ENCODING_24BIT_INT_MASK = 0xFF;
  private static final int LP_ENCODING_32BIT_INT = 0xF3;
  private static final int LP_ENCODING_32BIT_INT_MASK = 0xFF;
  private static final int LP_ENCODING_64BIT_INT = 0xF4;
  private static final int LP_ENCODING_64BIT_INT_MASK = 0xFF;
  private static final int LP_ENCODING_32BIT_STR = 0xF0;
  private static final int LP_ENCODING_32BIT_STR_MASK = 0xFF;

  private final byte[] envelope;

  ListpackList(byte[] envelope) {
    this.envelope = envelope;
  }

  private class ListpackParser {
    private int pos = 0;
    private List<byte[]> list = new ArrayList<byte[]>();

    private void decodeElement() {
      int b = envelope[pos++] & 0xff;

      // Handle the string cases first.
      int strLen = 0;

      if ((b & LP_ENCODING_6BIT_STR_MASK) == LP_ENCODING_6BIT_STR) {
        // 10|xxxxxx with x being the str length.
        strLen = b & ~LP_ENCODING_6BIT_STR_MASK;
      } else if ((b & LP_ENCODING_12BIT_STR_MASK) == LP_ENCODING_12BIT_STR) {
        // 1110|xxxxx yyyyyyyy str len up to 4095.
        strLen = ((int)envelope[pos++] & 0xff)
            |     (b & 0xff & ~LP_ENCODING_12BIT_STR_MASK) << 8;
      } else if ((b & LP_ENCODING_32BIT_STR_MASK) == LP_ENCODING_32BIT_STR) {
        // 1100|0000 subencoding.
        strLen = ((int)envelope[pos++] & 0xff) <<  0
            |    ((int)envelope[pos++] & 0xff) <<  8
            |    ((int)envelope[pos++] & 0xff) << 16
            |     (int)envelope[pos++]         << 24;
      }

      if (strLen > 0) {
        pos += strLen;
        list.add(Arrays.copyOfRange(envelope, pos - strLen, pos));
        pos += getLenBytes(strLen);
        return;
      }

      // Handle the ints.
      long val, negStart, negMax;

      if ((b & LP_ENCODING_7BIT_UINT_MASK) == LP_ENCODING_7BIT_UINT) {
        // Small number encoded in a single byte.
        list.add(String.valueOf(b & ~LP_ENCODING_7BIT_UINT_MASK).getBytes(ASCII));
        pos++;
        // Return immediately since 7-bit ints are never negative.
        return;
      } else if ((b & LP_ENCODING_13BIT_INT_MASK) == LP_ENCODING_13BIT_INT) { // 110|xxxxxx
        // yyyyyyyy
        val = (b & 0xff & ~LP_ENCODING_13BIT_INT_MASK) << 8 | envelope[pos++] & 0xff;
        negStart = 1 << 12;
        negMax = (1 << 13) - 1;
      } else if ((b & LP_ENCODING_16BIT_INT_MASK) == LP_ENCODING_16BIT_INT) { // 1111|0001
        val = ((long) envelope[pos++] & 0xff) | ((long) envelope[pos++] & 0xff) << 8;
        negStart = 1 << 15;
        negMax = (1 << 16) - 1;
      } else if ((b & LP_ENCODING_24BIT_INT_MASK) == LP_ENCODING_24BIT_INT) { // 1100|0010
        val = ((long) envelope[pos++] & 0xff) | ((long) envelope[pos++] & 0xff) << 8
            | ((long) envelope[pos++] & 0xff) << 16;
        negStart = 1L << 23;
        negMax = (1L << 24) - 1;
      } else if ((b & LP_ENCODING_32BIT_INT_MASK) == LP_ENCODING_32BIT_INT) { // 1100|0011
        val = ((long) envelope[pos++] & 0xff) | ((long) envelope[pos++] & 0xff) << 8
            | ((long) envelope[pos++] & 0xff) << 16
            | ((long) envelope[pos++] & 0xff) << 24;
        negStart = 1L << 31;
        negMax = (1L << 32) - 1;
      } else if ((b & LP_ENCODING_64BIT_INT_MASK) == LP_ENCODING_64BIT_INT) { // 1100|0100
        val = ((long) envelope[pos++] & 0xff) | ((long) envelope[pos++] & 0xff) << 8
            | ((long) envelope[pos++] & 0xff) << 16
            | ((long) envelope[pos++] & 0xff) << 24
            | ((long) envelope[pos++] & 0xff) << 32
            | ((long) envelope[pos++] & 0xff) << 40
            | ((long) envelope[pos++] & 0xff) << 48
            | ((long) envelope[pos++] & 0xff) << 56;
        // Since a long is 64 bits, no negative correction is needed.
        list.add(String.valueOf(val).getBytes(ASCII));
        pos++;
        return;
      } else {
        throw new RuntimeException("Invalid listpack envelope encoding");
      }

      // Convert to two's complement if value is negative.
      if (val >= negStart) {

        long diff = negMax - val;
        val = diff;
        val = -val - 1;
      }
      // Ints always have a entity size of one byte.
      pos++;
      list.add(String.valueOf(val).getBytes(ASCII));
    }

    private int getLenBytes(int len) {
      if (len < 128) {
        return 1;
      } else if (len < 16384) {
        return 2;
      } else if (len < 2097152) {
        return 3;
      } else if (len < 268435456) {
        return 4;
      } else {
        return 5;
      }
    }
  }

  @Override
  protected List<byte[]> realize() {
    // The structure of the listpack is:
    // <tot-bytes> <num-elements> <element-1> ... <element-N> <listpack-end-byte>
    // Where each element is of the structure:
    // <encoding-type><element-data><element-tot-len>.
    // Reference: https://github.com/antirez/listpack/blob/master/listpack.md

    ListpackParser listpackParser = new ListpackParser();
    // Skip 32-bit integer for the total number of bytes in listpack.
    listpackParser.pos += 4;
    int numElements = ((int) envelope[listpackParser.pos++] & 0xff) << 0
        | ((int) envelope[listpackParser.pos++] & 0xff) << 8;

    for (int i = 0; i < numElements; i++) {
      listpackParser.decodeElement();
    }
    if ((envelope[listpackParser.pos] & 0xff) != 0xff) {
      throw new IllegalStateException("Listpack did not end with 0xff byte.");
    }
    return listpackParser.list;
  }
}
