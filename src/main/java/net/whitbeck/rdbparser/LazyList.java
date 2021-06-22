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

import java.util.AbstractSequentialList;
import java.util.List;
import java.util.ListIterator;

abstract class LazyList<T> extends AbstractSequentialList<T> {

  private List<T> list = null;

  protected abstract List<T> realize();

  @Override
  public ListIterator<T> listIterator(int index) {
    if (list == null){
      list = realize();
    }
    return list.listIterator(index);
  }

  @Override
  public int size() {
    if (list == null){
      list = realize();
    }
    return list.size();
  }

}
