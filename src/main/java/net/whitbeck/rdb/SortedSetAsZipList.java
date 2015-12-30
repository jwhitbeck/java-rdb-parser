package net.whitbeck.rdb;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

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
  public List<byte[]> get() {
    List<byte[]> values = super.get();
    // fix the "+inf", "-inf", and "nan" values
    for (ListIterator<byte[]> i = values.listIterator(); i.hasNext(); ) {
      byte[] val = i.next();
      if (Arrays.equals(val, POS_INF_BYTES)) {
        i.set(DoubleBytes.POSITIVE_INFINITY);
      } else if (Arrays.equals(val, NEG_INF_BYTES)) {
        i.set( DoubleBytes.NEGATIVE_INFINITY);
      } else if (Arrays.equals(val, NAN_BYTES)) {
        i.set(DoubleBytes.NaN);
      }
    }
    return values;
  }
}
