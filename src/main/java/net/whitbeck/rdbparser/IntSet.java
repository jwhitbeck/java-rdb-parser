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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

final class IntSet extends LazyList<byte[]> {

  private static final Charset ASCII = Charset.forName("ASCII");

  private final byte[] envelope;

  IntSet(byte[] envelope) {
    this.envelope = envelope;
  }

  private int readIntAt(int pos) {
    return ((int)envelope[pos++] & 0xff) <<  0
         | ((int)envelope[pos++] & 0xff) <<  8
         | ((int)envelope[pos++] & 0xff) << 16
         | ((int)envelope[pos++] & 0xff) << 24;
  }

  private int getEncoding() {
    // Encoding can take three values: 2, 4, or 8, stored as a little-endian 32 bit integer.
    return readIntAt(0);
  }

  private int getNumInts() {
    // Number of ints is stored as a little-endian 32 bit integer, stored right after the encoding.
    return readIntAt(4);
  }

  private List<byte[]> read16BitInts(int num) {
    List<byte[]> ints = new ArrayList<byte[]>(num);
    int pos = 8; // skip the encoding and num ints
    for (int i = 0; i < num; ++i) {
      long val = ((long)envelope[pos++] & 0xff) << 0
               |  (long)envelope[pos++]         << 8;
      ints.add(String.valueOf(val).getBytes(ASCII));
    }
    return ints;
  }

  private List<byte[]> read32BitInts(int num) {
    List<byte[]> ints = new ArrayList<byte[]>(num);
    int pos = 8; // skip the encoding and num ints
    for (int i = 0; i < num; ++i) {
      long val = ((long)envelope[pos++] & 0xff) <<  0
               | ((long)envelope[pos++] & 0xff) <<  8
               | ((long)envelope[pos++] & 0xff) << 16
               | ((long)envelope[pos++])        << 24;
      ints.add(String.valueOf(val).getBytes(ASCII));
    }
    return ints;
  }

  private List<byte[]> read64BitInts(int num) {
    List<byte[]> ints = new ArrayList<byte[]>(num);
    int pos = 8; // skip the encoding and num ints
    for (int i = 0; i < num; ++i) {
      long val = ((long)envelope[pos++] & 0xff) <<  0
               | ((long)envelope[pos++] & 0xff) <<  8
               | ((long)envelope[pos++] & 0xff) << 16
               | ((long)envelope[pos++] & 0xff) << 24
               | ((long)envelope[pos++] & 0xff) << 32
               | ((long)envelope[pos++] & 0xff) << 40
               | ((long)envelope[pos++] & 0xff) << 48
               | (long)envelope[pos++]        << 56;
      ints.add(String.valueOf(val).getBytes(ASCII));
    }
    return ints;
  }

  @Override
  protected List<byte[]> realize() {
    int encoding = getEncoding();
    int num = getNumInts();
    switch (encoding) {
      case 2:
        return read16BitInts(num);
      case 4:
        return read32BitInts(num);
      case 8:
        return read64BitInts(num);
      default:
        throw new IllegalStateException("Unknown intset encoding");
    }
  }

}
