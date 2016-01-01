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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

class ZipList implements LazyList {

  private final static Charset ASCII = Charset.forName("ASCII");
  private final byte[] envelope;

  ZipList(byte[] envelope) {
    this.envelope = envelope;
  }

  @Override
  public List<byte[]> get() {
    // skip the first 8 bytes representing the total size in bytes of the ziplist and the offset to the last
    // element.
    int pos = 8;
    // read number of elements as a 2 byte little-endian integer
    int n = ((((int)envelope[pos++] & 0xff) << 0) |
             (((int)envelope[pos++] & 0xff) << 8));
    List<byte[]> list = new ArrayList<byte[]>(n);
    int i = 0;
    while (i < n) {
      // skip length of previous entry. If len is <= 253, it represents the length of the previous entry,
      // otherwise, they next four bytes are used to store the length
      int prevLen = (int)envelope[pos++] & 0xff;
      if (prevLen > 0xfc) {
        pos += 4;
      }
      int special = (int)envelope[pos++] & 0xff;
      int top2bits = special >> 6;
      int len;
      byte[] val;
      switch (top2bits) {
      case 0: // string value with length less than or equal to 63 bytes (6 bits)
        len = special & 0x3f;
        val = new byte[len];
        System.arraycopy(envelope, pos, val, 0, len);
        pos += len;
        list.add(val);
        break;
      case 1: // String value with length less than or equal to 16383 bytes (14 bits).
        len = ((special & 0x3f) << 8) | ((int)envelope[pos++] & 0xff);
        val = new byte[len];
        System.arraycopy(envelope, pos, val, 0, len);
        pos += len;
        list.add(val);
        break;
      case 2: // String value with length greater than or equal to 16384 bytes. Length is read from 4
              // following bytes.
        len = ((((int)envelope[pos++] & 0xff) <<  0) |
               (((int)envelope[pos++] & 0xff) <<  8) |
               (((int)envelope[pos++] & 0xff) << 16) |
               (((int)envelope[pos++] & 0xff) << 24));
        val = new byte[len];
        System.arraycopy(envelope, pos, val, 0, len);
        pos += len;
        list.add(val);
        break;
      case 3: // integer encodings
        int flag = (special & 0x30) >> 4;
        long v;
        switch (flag) {
        case 0: // read next 2 bytes as a 16 bit signed integer
          v = (((long)envelope[pos++] & 0xff) |
               ((long)envelope[pos++] << 8));
          list.add(String.valueOf(v).getBytes(ASCII));
          break;
        case 1: // read next 4 bytes as a 32 bit signed integer
          v = ((((long)envelope[pos++] & 0xff) <<  0) |
               (((long)envelope[pos++] & 0xff) <<  8) |
               (((long)envelope[pos++] & 0xff) << 16) |
               (((long)envelope[pos++])        << 24));
          list.add(String.valueOf(v).getBytes(ASCII));
          break;
        case 2: // read next 8 as a 64 bit signed integer
          v = ((((long)envelope[pos++] & 0xff) <<  0) |
               (((long)envelope[pos++] & 0xff) <<  8) |
               (((long)envelope[pos++] & 0xff) << 16) |
               (((long)envelope[pos++] & 0xff) << 24) |
               (((long)envelope[pos++] & 0xff) << 32) |
               (((long)envelope[pos++] & 0xff) << 40) |
               (((long)envelope[pos++] & 0xff) << 48) |
               (((long)envelope[pos++])        << 56));
          list.add(String.valueOf(v).getBytes(ASCII));
          break;
        case 3:
          int loBits = (special & 0x0f);
          switch (loBits) {
          case 0: // read next 3 bytes as a 24 bit signed integer
            v = ((((long)envelope[pos++] & 0xff) <<  0) |
                 (((long)envelope[pos++] & 0xff) <<  8) |
                 (((long)envelope[pos++])        << 16));
            list.add(String.valueOf(v).getBytes(ASCII));
            break;
          case 0x0e: // read next byte as an 8 bit signed integer
            v = (long)envelope[pos++];
            list.add(String.valueOf(v).getBytes(ASCII));
            break;
          default: // a immediate 4 bit unsigned integer between 0 and 12. Substract 1 as the range is
                   // actually between 1 and 13.
            list.add(String.valueOf(loBits - 1).getBytes(ASCII));
            break;
          }
        }
        break;
      default: // never reached
      }
      i += 1;
    }
    return list;
  }
}
