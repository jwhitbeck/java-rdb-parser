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

import java.util.ArrayList;
import java.util.List;

final class QuickList extends LazyList<byte[]> {

  private final List<byte[]> ziplists;

  QuickList(List<byte[]> ziplists) {
    this.ziplists = ziplists;
  }

  @Override
  protected List<byte[]> realize() {
    List<byte[]> list = new ArrayList<byte[]>();
    for (byte[] envelope : ziplists) {
      list.addAll(new ZipList(envelope).realize());
    }
    return list;
  }
}
