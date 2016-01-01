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
import java.util.List;
import java.util.ArrayList;

final class IntSet implements LazyList {

  private final static Charset ASCII = Charset.forName("ASCII");

  private final byte[] envelope;

  IntSet(byte[] envelope) {
    this.envelope = envelope;
  }

  private int readIntAt(int p) {
    return ((((int)envelope[p++] & 0xff) <<  0) |
            (((int)envelope[p++] & 0xff) <<  8) |
            (((int)envelope[p++] & 0xff) << 16) |
            (((int)envelope[p++] & 0xff) << 24));
  }

  private int getEncoding() {
    // encoding can take three values: 2, 4, 8, stored as a little-endian 32 bit integer
    return readIntAt(0);
  }

  private int getNumInts() {
    // number of ints is stored as a little-endian 32 bit integer, stored right after the encoding
    return readIntAt(4);
  }

  private List<byte[]> read16BitInts(int n) {
    List<byte[]> ints = new ArrayList<byte[]>(n);
    int pos = 8; // skip the encoding and num ints
    for (int i=0; i<n; ++i) {
      long l = ((((long)envelope[pos++] & 0xff) << 0) |
                (((long)envelope[pos++])        << 8));;
      ints.add(String.valueOf(l).getBytes(ASCII));
    }
    return ints;
  }

  private List<byte[]> read32BitInts(int n) {
    List<byte[]> ints = new ArrayList<byte[]>(n);
    int pos = 8; // skip the encoding and num ints
    for (int i=0; i<n; ++i) {
      long l = ((((long)envelope[pos++] & 0xff) <<  0) |
                (((long)envelope[pos++] & 0xff) <<  8) |
                (((long)envelope[pos++] & 0xff) << 16) |
                (((long)envelope[pos++])        << 24));;
      ints.add(String.valueOf(l).getBytes(ASCII));
    }
    return ints;
  }

  private List<byte[]> read64BitInts(int n) {
    List<byte[]> ints = new ArrayList<byte[]>(n);
    int pos = 8; // skip the encoding and num ints
    for (int i=0; i<n; ++i) {
      long l = ((((long)envelope[pos++] & 0xff) <<  0) |
                (((long)envelope[pos++] & 0xff) <<  8) |
                (((long)envelope[pos++] & 0xff) << 16) |
                (((long)envelope[pos++] & 0xff) << 24) |
                (((long)envelope[pos++] & 0xff) << 32) |
                (((long)envelope[pos++] & 0xff) << 40) |
                (((long)envelope[pos++] & 0xff) << 48) |
                (((long)envelope[pos++])        << 56));
      ints.add(String.valueOf(l).getBytes(ASCII));
    }
    return ints;
  }

  @Override
  public List<byte[]> get() {
    int encoding = getEncoding();
    int n = getNumInts();
    switch (encoding) {
    case 2:
      return read16BitInts(n);
    case 4:
      return read32BitInts(n);
    case 8:
      return read64BitInts(n);
    default:
      throw new IllegalStateException("Unknown intset encoding");
    }
  }

}
