package net.whitbeck.rdb;

import java.nio.charset.Charset;

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

  private byte[][] read16BitInts(int n) {
    byte[][] ints = new byte[n][];
    int pos = 8; // skip the encoding and num ints
    for (int i=0; i<n; ++i) {
      long l = ((((long)envelope[pos++] & 0xff) << 0) |
                (((long)envelope[pos++])        << 8));;
      ints[i] = String.valueOf(l).getBytes(ASCII);
    }
    return ints;
  }

  private byte[][] read32BitInts(int n) {
    byte[][] ints = new byte[n][];
    int pos = 8; // skip the encoding and num ints
    for (int i=0; i<n; ++i) {
      long l = ((((long)envelope[pos++] & 0xff) <<  0) |
                (((long)envelope[pos++] & 0xff) <<  8) |
                (((long)envelope[pos++] & 0xff) << 16) |
                (((long)envelope[pos++])        << 24));;
      ints[i] = String.valueOf(l).getBytes(ASCII);
    }
    return ints;
  }

  private byte[][] read64BitInts(int n) {
    byte[][] ints = new byte[n][];
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
      ints[i] = String.valueOf(l).getBytes(ASCII);
    }
    return ints;
  }

  @Override
  public byte[][] get() {
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
