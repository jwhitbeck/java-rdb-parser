package net.whitbeck.rdb;

import java.nio.charset.Charset;

final class DoubleBytes {

  private final static Charset ASCII = Charset.forName("ASCII");

  final static byte[]
    POSITIVE_INFINITY = String.valueOf(Double.POSITIVE_INFINITY).getBytes(ASCII),
    NEGATIVE_INFINITY = String.valueOf(Double.NEGATIVE_INFINITY).getBytes(ASCII),
    NaN = String.valueOf(Double.NaN).getBytes(ASCII);

}
