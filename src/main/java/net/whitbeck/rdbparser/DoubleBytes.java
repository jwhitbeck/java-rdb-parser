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

final class DoubleBytes {

  private static final Charset ASCII = Charset.forName("ASCII");

  static final byte[] POSITIVE_INFINITY = String.valueOf(Double.POSITIVE_INFINITY).getBytes(ASCII);
  static final byte[] NEGATIVE_INFINITY = String.valueOf(Double.NEGATIVE_INFINITY).getBytes(ASCII);
  static final byte[] NaN = String.valueOf(Double.NaN).getBytes(ASCII);

}
