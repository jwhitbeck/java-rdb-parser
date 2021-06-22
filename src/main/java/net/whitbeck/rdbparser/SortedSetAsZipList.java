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
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

final class SortedSetAsZipList extends LazyList<byte[]> {

  private static final Charset ASCII = Charset.forName("ASCII");

  private static final byte[] POS_INF_BYTES = "inf".getBytes(ASCII);
  private static final byte[] NEG_INF_BYTES = "-inf".getBytes(ASCII);
  private static final byte[] NAN_BYTES = "nan".getBytes(ASCII);

  private final byte[] envelope;

  SortedSetAsZipList(byte[] envelope) {
    this.envelope = envelope;
  }

  @Override
  protected List<byte[]> realize() {
    List<byte[]> values = new ZipList(envelope).realize();
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
