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

package net.whitbeck.rdbparser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reads entries from a Redis RDB file, one at a time.
 *
 * @author John Whitbeck
 */
public final class RdbParser implements AutoCloseable {

  private final static Charset ASCII = Charset.forName("ASCII");

  private static final int
    EOF = 0xff,
    DB_SELECT = 0xfe,
    KEY_VALUE_SECS = 0xfd,
    KEY_VALUE_MS = 0xfc;

  private static final int BUFFER_SIZE = 8 * 1024;

  private final ReadableByteChannel ch;
  private final ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);

  /* Parsing state */
  private int version;
  private boolean isInitialized = false;
  private boolean hasNext = false;

  public RdbParser(ReadableByteChannel ch) {
    this.ch = ch;
  }

  public RdbParser(Path path) throws IOException {
    this.ch = FileChannel.open(path, StandardOpenOption.READ);
  }

  public RdbParser(File file) throws IOException {
    this(file.toPath());
  }

  public RdbParser(InputStream inputStream) throws IOException {
    this(Channels.newChannel(inputStream));
  }

  private void fillBuffer() throws IOException {
    buf.clear();
    if (ch.read(buf) == -1) {
      throw new IOException("Attempting to read past channel end-of-stream.");
    }
    buf.flip();
  }

  private int readByte() throws IOException {
    if (!buf.hasRemaining()) {
      fillBuffer();
    }
    return buf.get() & 0xff;
  }

  private byte[] readBytes(int n) throws IOException {
    int rem = n;
    int pos = 0;
    byte[] bs = new byte[n];
    while (rem > 0) {
      int avail = buf.remaining();
      if (avail >= rem) {
        buf.get(bs, pos, rem);
        pos += rem;
        rem = 0;
      } else {
        buf.get(bs, pos, avail);
        pos += avail;
        rem -= avail;
        fillBuffer();
      }
    }
    return bs;
  }

  private String readMagicNumber() throws IOException {
    return new String(readBytes(5), ASCII);
  }

  private int readVersion() throws IOException {
    return Integer.parseInt(new String(readBytes(4), ASCII));
  }

  private void init() throws IOException {
    fillBuffer();
    if (!readMagicNumber().equals("REDIS")) {
      throw new IllegalStateException("Not a valid redis RDB file");
    }
    version = readVersion();
    if (version < 1 || version > 6) {
      throw new IllegalStateException("Unknown version");
    }
    isInitialized = true;
    hasNext = true;
  }


  /**
   * Returns the next Entry from the underlying file or stream.
   *
   * @return the next entry
   *
   * @throws IOException
   */
  public Entry readNext() throws IOException {
    if (!hasNext) {
      if (!isInitialized) {
        init();
        return readNext();
      } else { // EOF reached
        return null;
      }
    }
    int b = readByte();
    switch (b) {
    case EOF:
      return readEOF();
    case DB_SELECT:
      return readDbSelect();
    case KEY_VALUE_SECS:
      return readEntrySeconds();
    case KEY_VALUE_MS:
      return readEntryMillis();
    default:
      return readEntry(null, b);
    }
  }

  private byte[] readChecksum() throws IOException {
    return readBytes(8);
  }

  private byte[] getEmptyChecksum() {
    return new byte[8];
  }

  private Eof readEOF() throws IOException {
    byte[] checksum = (version >= 5)? readChecksum() : getEmptyChecksum();
    hasNext = false;
    return new Eof(checksum);
  }

  private DbSelect readDbSelect() throws IOException {
    return new DbSelect(readLength());
  }

  private long readLength() throws IOException {
    int b = readByte();
    // the first two bits determine the encoding
    int flag = (b & 0xc0) >> 6;
    switch (flag) {
    case 0: // length is read from the lower 6 bits
      return b & 0x3f;
    case 1: // one additional byte is read for a 14 bit encoding
      return (((long)b & 0x3f) << 8) | ((long)readByte() & 0xff);
    case 2: // read next four bytes as unsigned big-endian
      byte[] bs = readBytes(4);
      return ((((long)bs[0] & 0xff) << 24) |
              (((long)bs[1] & 0xff) << 16) |
              (((long)bs[2] & 0xff) <<  8) |
              (((long)bs[3] & 0xff) <<  0));
    default:
      throw new IllegalStateException("Expected a length, but got a special string encoding.");
    }
  }

  private byte[] readStringEncoded() throws IOException {
    int b = readByte();
    // the first two bits determine the encoding
    int flag = (b & 0xc0) >> 6;
    int len;
    switch (flag) {
    case 0: // length is read from the lower 6 bits
      len = (b & 0x3f);
      return readBytes(len);
    case 1: // one additional byte is read for a 14 bit encoding
      len = ((b & 0x3f) << 8) | (readByte() & 0xff);
      return readBytes(len);
    case 2: // read next four bytes as unsigned big-endian
      byte[] bs = readBytes(4);
      len = ((((int)bs[0] & 0xff) << 24) |
             (((int)bs[1] & 0xff) << 16) |
             (((int)bs[2] & 0xff) <<  8) |
             (((int)bs[3] & 0xff) <<  0));
      if (len < 0) {
        throw new IllegalStateException("Strings longer than " + Integer.MAX_VALUE +
                                        "bytes are not supported.");
      }
      return readBytes(len);
    case 3:
      return readSpecialStringEncoded(b & 0x3f);
    default: // never reached
      return null;
    }
  }

  private byte[] readInteger8Bits() throws IOException {
    return String.valueOf(readByte()).getBytes(ASCII);
  }

  private byte[] readInteger16Bits() throws IOException {
    long l = ((((long)readByte() & 0xff) << 0) |
              (((long)readByte() & 0xff) << 8));
    return String.valueOf(l).getBytes(ASCII);
  }

  private byte[] readInteger32Bits() throws IOException {
    byte[] bs = readBytes(4);
    long l = ((((long)bs[3] & 0xff) << 24) |
              (((long)bs[2] & 0xff) << 16) |
              (((long)bs[1] & 0xff) <<  8) |
              (((long)bs[0] & 0xff) <<  0));
    return String.valueOf(l).getBytes(ASCII);
  }

  private byte[] readLzfString() throws IOException {
    int clen = (int)readLength();
    int ulen = (int)readLength();
    byte[] src = readBytes(clen);
    byte[] dest = new byte[ulen];
    Lzf.expand(src, dest);
    return dest;
  }

  private byte[] readDoubleString() throws IOException {
    int len = readByte();
    switch (len) {
    case 0xff:
      return DoubleBytes.NEGATIVE_INFINITY;
    case 0xfe:
      return DoubleBytes.POSITIVE_INFINITY;
    case 0xfd:
      return DoubleBytes.NaN;
    default:
      return readBytes(len);
    }
  }

  private byte[] readSpecialStringEncoded(int type) throws IOException {
    switch (type) {
    case 0:
      return readInteger8Bits();
    case 1:
      return readInteger16Bits();
    case 2:
      return readInteger32Bits();
    case 3:
      return readLzfString();
    default:
      throw new IllegalStateException("Unknown special encoding: " + type);
    }
  }

  private KeyValuePair readEntrySeconds() throws IOException {
    return readEntry(readBytes(4), readByte());
  }

  private KeyValuePair readEntryMillis() throws IOException {
    return readEntry(readBytes(8), readByte());
  }

  private KeyValuePair readEntry(byte[] ts, int valueType) throws IOException {
    byte[] key = readStringEncoded();
    switch (valueType) {
    case 0:
      return readValue(ts, key);
    case 1:
      return readList(ts, key);
    case 2:
      return readSet(ts, key);
    case 3:
      return readSortedSet(ts, key);
    case 4:
      return readHash(ts, key);
    case 9:
      return readZipMap(ts, key);
    case 10:
      return readZipList(ts, key);
    case 11:
      return readIntSet(ts, key);
    case 12:
      return readSortedSetAsZipList(ts, key);
    case 13:
      return readHashmapAsZipList(ts, key);
    default:
      throw new UnsupportedOperationException("Unknown value type: " + valueType);
    }
  }

  private KeyValuePair readValue(byte[] ts, byte[] key) throws IOException {
    return new KeyValuePair(ValueType.VALUE, ts, key, Arrays.asList(readStringEncoded()));
  }

  private KeyValuePair readList(byte[] ts, byte[] key) throws IOException {
    long len = readLength();
    if (len > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Lists with more than " + Integer.MAX_VALUE +
                                         " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> list = new ArrayList<byte[]>(size);
    for (int i=0; i<size; ++i) {
      list.add(readStringEncoded());
    }
    return new KeyValuePair(ValueType.LIST, ts, key, list);
  }

  private KeyValuePair readSet(byte[] ts, byte[] key) throws IOException {
    long len = readLength();
    if (len > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Sets with more than " + Integer.MAX_VALUE +
                                         " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> set = new ArrayList<byte[]>(size);
    for (int i=0; i<size; ++i) {
      set.add(readStringEncoded());
    }
    return new KeyValuePair(ValueType.SET, ts, key, set);
  }

  private KeyValuePair readSortedSet(byte[] ts, byte[] key) throws IOException {
    long len = readLength();
    if (len > (Integer.MAX_VALUE / 2)) {
      throw new IllegalArgumentException("SortedSets with more than " + (Integer.MAX_VALUE / 2) +
                                         " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> valueScoresPairs = new ArrayList<byte[]>(2 * size);
    for (int i=0; i<size; ++i) {
      valueScoresPairs.add(readStringEncoded());
      valueScoresPairs.add(readDoubleString());
    }
    return new KeyValuePair(ValueType.SORTED_SET, ts, key, valueScoresPairs);
  }

  private KeyValuePair readHash(byte[] ts, byte[] key) throws IOException {
    long len = readLength();
    if (len > (Integer.MAX_VALUE / 2)) {
      throw new IllegalArgumentException("Hashes with more than " + (Integer.MAX_VALUE / 2) +
                                         " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> kvPairs = new ArrayList<byte[]>(2 * size);
    for (int i=0; i<size; ++i) {
      kvPairs.add(readStringEncoded());
      kvPairs.add(readStringEncoded());
    }
    return new KeyValuePair(ValueType.HASH, ts, key, kvPairs);
  }

  private KeyValuePair readZipMap(byte[] ts, byte[] key) throws IOException {
    throw new UnsupportedOperationException("Parsing zipmaps (deprecated as of redis 2.6) " +
                                            "is not supported!");
  }

  private KeyValuePair readZipList(byte[] ts, byte[] key) throws IOException {
    return new KeyValuePair(ValueType.ZIPLIST, ts, key, new ZipList(readStringEncoded()));
  }

  private KeyValuePair readIntSet(byte[] ts, byte[] key) throws IOException {
    return new KeyValuePair(ValueType.INTSET, ts, key, new IntSet(readStringEncoded()));
  }

  private KeyValuePair readSortedSetAsZipList(byte[] ts, byte[] key) throws IOException {
    return new KeyValuePair(ValueType.SORTED_SET_AS_ZIPLIST, ts, key,
                            new SortedSetAsZipList(readStringEncoded()));
  }

  private KeyValuePair readHashmapAsZipList(byte[] ts, byte[] key) throws IOException {
    return new KeyValuePair(ValueType.HASHMAP_AS_ZIPLIST, ts, key, new ZipList(readStringEncoded()));
  }

  /**
   * Closes the underlying file or stream.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    ch.close();
  }

}
