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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
 * <p>Reads entries from a Redis RDB file, one at a time.
 *
 * @author John Whitbeck
 */
public final class RdbParser implements AutoCloseable {

  private static final Charset ASCII = Charset.forName("ASCII");

  private static final int EOF = 0xff;
  private static final int SELECTDB = 0xfe;
  private static final int EXPIRETIME = 0xfd;
  private static final int EXPIRETIME_MS = 0xfc;
  private static final int RESIZEDB = 0xfb;
  private static final int AUX = 0xfa;
  private static final int FREQ = 0xf9;
  private static final int IDLE = 0xf8;
  private static final int MODULE_AUX = 0xf7;

  private static final int BUFFER_SIZE = 8 * 1024;

  private final ReadableByteChannel ch;
  private final ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);

  /* Parsing state */
  private int version;
  private long bytesBuffered = 0;
  private boolean isInitialized = false;
  private KeyValuePair nextEntry = null;
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

  public RdbParser(String filename) throws IOException {
    this(new File(filename));
  }

  /**
   * Returns the version of the RDB file being parsed.
   *
   * @return the RDB file version
   */
  public int getRdbVersion() {
    return version;
  }

  private void fillBuffer() throws IOException {
    buf.clear();
    long n = ch.read(buf);
    if (n == -1) {
      throw new IOException("Attempting to read past channel end-of-stream.");
    }
    bytesBuffered += n;
    buf.flip();
  }

  private int readByte() throws IOException {
    if (!buf.hasRemaining()) {
      fillBuffer();
    }
    return buf.get() & 0xff;
  }

  private int readSignedByte() throws IOException {
    if (!buf.hasRemaining()) {
      fillBuffer();
    }
    return buf.get();
  }

  private byte[] readBytes(int numBytes) throws IOException {
    int rem = numBytes;
    int pos = 0;
    byte[] bs = new byte[numBytes];
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
    if (version < 1 || version > 9) {
      throw new IllegalStateException("Unknown version");
    }
    nextEntry = new KeyValuePair();
    hasNext = true;
    isInitialized = true;
  }

  /**
   * <p>Returns the number of bytes parsed from the underlying file or stream by successive calls of
   * the {@link #readNext} method.
   *
   * <p>As RdbParser uses a buffer internally, the returned value will be slightly smaller than the
   * total number of bytes buffered from the underlying file or stream.
   *
   * @return the number of bytes parsed so far.
   */
  public long bytesParsed() {
    return bytesBuffered - buf.remaining();
  }

  /**
   * Returns the next Entry from the underlying file or stream.
   *
   * @return the next entry
   *
   * @throws IOException if there is an error reading from the underlying channel.
   */
  public Entry readNext() throws IOException {
    while (true) {
      if (!hasNext) {
        if (!isInitialized) {
          init();
          continue;
        } else { // EOF reached
          return null;
        }
      }
      int valueType = readByte();
      switch (valueType) {
        case EOF:
          return readEof();
        case SELECTDB:
          return readSelectDb();
        case RESIZEDB:
          return readResizeDb();
        case AUX:
          return readAuxField();
        case EXPIRETIME:
          readExpireTime();
          continue;
        case EXPIRETIME_MS:
          readExpireTimeMillis();
          continue;
        case FREQ:
          readFreq();
          continue;
        case IDLE:
          readIdle();
          continue;
        case MODULE_AUX:
          throw new UnsupportedOperationException("Redis modules are not supported");
        default:
          readEntry(valueType);
          KeyValuePair entry = nextEntry;
          nextEntry = new KeyValuePair();
          return entry;
      }
    }
  }

  private byte[] readChecksum() throws IOException {
    return readBytes(8);
  }

  private byte[] getEmptyChecksum() {
    return new byte[8];
  }

  private Eof readEof() throws IOException {
    byte[] checksum = version >= 5 ? readChecksum() : getEmptyChecksum();
    hasNext = false;
    return new Eof(checksum);
  }

  private SelectDb readSelectDb() throws IOException {
    return new SelectDb(readLength());
  }

  private ResizeDb readResizeDb() throws IOException {
    return new ResizeDb(readLength(), readLength());
  }

  private AuxField readAuxField() throws IOException {
    return new AuxField(readStringEncoded(), readStringEncoded());
  }

  private void readFreq() throws IOException {
    nextEntry.freq = readByte();
  }

  private void readIdle() throws IOException {
    nextEntry.idle = readLength();
  }

  private long readLength() throws IOException {
    int firstByte = readByte();
    // The first two bits determine the encoding.
    int flag = (firstByte & 0xc0) >> 6;
    if (flag == 0) { // 00|XXXXXX: len is the last 6 bits of this byte.
      return firstByte & 0x3f;
    } else if (flag == 1) { // 01|XXXXXX: len is encoded on the next 14 bits.
      return (((long)firstByte & 0x3f) << 8) | ((long)readByte() & 0xff);
    } else if (firstByte == 0x80) {
      // 10|000000: len is a 32-bit integer encoded on the next 4 bytes.
      byte[] bs = readBytes(4);
      return ((long)bs[0] & 0xff) << 24
          | ((long)bs[1] & 0xff) << 16
          | ((long)bs[2] & 0xff) <<  8
          | ((long)bs[3] & 0xff) <<  0;
    } else if (firstByte == 0x81) {
      // 10|000001: len is a 64-bit integer encoded on the next 8 bytes.
      byte[] bs = readBytes(8);
      return ((long)bs[0] & 0xff) << 56
          | ((long)bs[1] & 0xff) << 48
          | ((long)bs[2] & 0xff) << 40
          | ((long)bs[3] & 0xff) << 32
          | ((long)bs[4] & 0xff) << 24
          | ((long)bs[5] & 0xff) << 16
          | ((long)bs[6] & 0xff) <<  8
          | ((long)bs[7] & 0xff) <<  0;
    } else {
      // 11|XXXXXX: special encoding.
      throw new IllegalStateException("Expected a length, but got a special string encoding.");
    }
  }

  private byte[] readStringEncoded() throws IOException {
    int firstByte = readByte();
    // the first two bits determine the encoding
    int flag = (firstByte & 0xc0) >> 6;
    int len;
    switch (flag) {
      case 0: // length is read from the lower 6 bits
        len = firstByte & 0x3f;
        return readBytes(len);
      case 1: // one additional byte is read for a 14 bit encoding
        len = ((firstByte & 0x3f) << 8) | (readByte() & 0xff);
        return readBytes(len);
      case 2: // read next four bytes as unsigned big-endian
        byte[] bs = readBytes(4);
        len = ((int)bs[0] & 0xff) << 24
            | ((int)bs[1] & 0xff) << 16
            | ((int)bs[2] & 0xff) <<  8
            | ((int)bs[3] & 0xff) <<  0;
        if (len < 0) {
          throw new IllegalStateException("Strings longer than " + Integer.MAX_VALUE
                                          + "bytes are not supported.");
        }
        return readBytes(len);
      case 3:
        return readSpecialStringEncoded(firstByte & 0x3f);
      default: // never reached
        return null;
    }
  }

  private byte[] readInteger8Bits() throws IOException {
    return String.valueOf(readSignedByte()).getBytes(ASCII);
  }

  private byte[] readInteger16Bits() throws IOException {
    long val = ((long)readByte() & 0xff) << 0
             |  (long)readSignedByte()   << 8; // Don't apply 0xff mask to preserve sign.
    return String.valueOf(val).getBytes(ASCII);
  }

  private byte[] readInteger32Bits() throws IOException {
    byte[] bs = readBytes(4);
    long val =  (long)bs[3]         << 24 // Don't apply 0xff mask to preserve sign.
             | ((long)bs[2] & 0xff) << 16
             | ((long)bs[1] & 0xff) <<  8
             | ((long)bs[0] & 0xff) <<  0;
    return String.valueOf(val).getBytes(ASCII);
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

  private void readExpireTime() throws IOException {
    nextEntry.expireTime = readBytes(4);
  }

  private void readExpireTimeMillis() throws IOException {
    nextEntry.expireTime = readBytes(8);
  }

  private void readEntry(int valueType) throws IOException {
    nextEntry.key = readStringEncoded();
    switch (valueType) {
      case 0:
        readValue();
        break;
      case 1:
        readList();
        break;
      case 2:
        readSet();
        break;
      case 3:
        readSortedSet();
        break;
      case 4:
        readHash();
        break;
      case 5:
        readSortedSet2();
        break;
      case 6: // Modules v1
      case 7: // Modules v2
        throw new UnsupportedOperationException("Redis modules are not supported");
      case 9:
        readZipMap();
        break;
      case 10:
        readZipList();
        break;
      case 11:
        readIntSet();
        break;
      case 12:
        readSortedSetAsZipList();
        break;
      case 13:
        readHashmapAsZipList();
        break;
      case 14:
        readQuickList();
        break;
      case 15: // Steam ListPacks
        throw new UnsupportedOperationException("Redis streams are not supported");
      default:
        throw new UnsupportedOperationException("Unknown value type: " + valueType);
    }
  }

  private void readValue() throws IOException {
    nextEntry.valueType = ValueType.VALUE;
    nextEntry.values = Arrays.asList(readStringEncoded());
  }

  private void readList() throws IOException {
    long len = readLength();
    if (len > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Lists with more than " + Integer.MAX_VALUE
                                         + " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> list = new ArrayList<byte[]>(size);
    for (int i = 0; i < size; ++i) {
      list.add(readStringEncoded());
    }
    nextEntry.valueType = ValueType.LIST;
    nextEntry.values = list;
  }

  private void readSet() throws IOException {
    long len = readLength();
    if (len > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Sets with more than " + Integer.MAX_VALUE
                                         + " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> set = new ArrayList<byte[]>(size);
    for (int i = 0; i < size; ++i) {
      set.add(readStringEncoded());
    }
    nextEntry.valueType = ValueType.SET;
    nextEntry.values = set;
  }

  private void readSortedSet() throws IOException {
    long len = readLength();
    if (len > (Integer.MAX_VALUE / 2)) {
      throw new IllegalArgumentException("SortedSets with more than " + (Integer.MAX_VALUE / 2)
                                         + " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> valueScoresPairs = new ArrayList<byte[]>(2 * size);
    for (int i = 0; i < size; ++i) {
      valueScoresPairs.add(readStringEncoded());
      valueScoresPairs.add(readDoubleString());
    }
    nextEntry.valueType = ValueType.SORTED_SET;
    nextEntry.values = valueScoresPairs;
  }

  private void readSortedSet2() throws IOException {
    long len = readLength();
    if (len > (Integer.MAX_VALUE / 2)) {
      throw new IllegalArgumentException("SortedSets with more than " + (Integer.MAX_VALUE / 2)
                                         + " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> valueScoresPairs = new ArrayList<byte[]>(2 * size);
    for (int i = 0; i < size; ++i) {
      valueScoresPairs.add(readStringEncoded());
      valueScoresPairs.add(readBytes(8));
    }
    nextEntry.valueType = ValueType.SORTED_SET2;
    nextEntry.values = valueScoresPairs;
  }

  private void readHash() throws IOException {
    long len = readLength();
    if (len > (Integer.MAX_VALUE / 2)) {
      throw new IllegalArgumentException("Hashes with more than " + (Integer.MAX_VALUE / 2)
                                         + " elements are not supported.");
    }
    int size = (int)len;
    List<byte[]> kvPairs = new ArrayList<byte[]>(2 * size);
    for (int i = 0; i < size; ++i) {
      kvPairs.add(readStringEncoded());
      kvPairs.add(readStringEncoded());
    }
    nextEntry.valueType = ValueType.HASH;
    nextEntry.values = kvPairs;
  }

  private void readZipMap() throws IOException {
    nextEntry.valueType = ValueType.ZIPMAP;
    nextEntry.values = new ZipMap(readStringEncoded());
  }

  private void readZipList() throws IOException {
    nextEntry.valueType = ValueType.ZIPLIST;
    nextEntry.values = new ZipList(readStringEncoded());
  }

  private void readIntSet() throws IOException {
    nextEntry.valueType = ValueType.INTSET;
    nextEntry.values = new IntSet(readStringEncoded());
  }

  private void readSortedSetAsZipList() throws IOException {
    nextEntry.valueType = ValueType.SORTED_SET_AS_ZIPLIST;
    nextEntry.values = new SortedSetAsZipList(readStringEncoded());
  }

  private void readHashmapAsZipList() throws IOException {
    nextEntry.valueType = ValueType.HASHMAP_AS_ZIPLIST;
    nextEntry.values = new ZipList(readStringEncoded());
  }

  private void readQuickList() throws IOException {
    long len = readLength();
    if (len > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Quicklists with more than " + Integer.MAX_VALUE
                                         + " nested Ziplists are not supported.");
    }
    int size = (int)len;
    List<byte[]> ziplists = new ArrayList<byte[]>(size);
    for (int i = 0; i < size; ++i) {
      ziplists.add(readStringEncoded());
    }
    nextEntry.valueType = ValueType.QUICKLIST;
    nextEntry.values = new QuickList(ziplists);
  }

  /**
   * Closes the underlying file or stream.
   *
   * @throws IOException from closing the underlying channel.
   */
  @Override
  public void close() throws IOException {
    ch.close();
  }

  /**
   * Parses the raw score of an element in a {@link ValueType#SORTED_SET} or
   * {@link ValueType#SORTED_SET_AS_ZIPLIST}.
   */
  public static double parseSortedSetScore(byte[] bytes) {
    return Double.parseDouble(new String(bytes, ASCII));
  }

  /**
   * Parses the raw score of an element in a {@link ValueType#SORTED_SET2}.
   */
  public static double parseSortedSet2Score(byte[] bytes) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
  }

}
