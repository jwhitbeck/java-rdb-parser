package net.whitbeck.rdb;

import java.nio.charset.Charset;
import java.util.Arrays;

class SortedSetAsZipList extends ZipList {

  private final static Charset ASCII = Charset.forName("ASCII");

  private static final byte[]
    POS_INF_BYTES = "inf".getBytes(ASCII),
    NEG_INF_BYTES = "-inf".getBytes(ASCII),
    NAN_BYTES = "nan".getBytes(ASCII);

  SortedSetAsZipList(byte[] envelope) {
    super(envelope);
  }

  @Override
  public byte[][] get() {
    byte[][] values = super.get();
    // fix the "+inf", "-inf", and "nan" values
    for (int i=1; i<values.length; i+=2) {
      if (Arrays.equals(values[i], POS_INF_BYTES)) {
        values[i] = DoubleBytes.POSITIVE_INFINITY;
      } else if (Arrays.equals(values[i], NEG_INF_BYTES)) {
        values[i] = DoubleBytes.NEGATIVE_INFINITY;
      } else if (Arrays.equals(values[i], NAN_BYTES)) {
        values[i] = DoubleBytes.NaN;
      }
    }
    return values;
  }
}
