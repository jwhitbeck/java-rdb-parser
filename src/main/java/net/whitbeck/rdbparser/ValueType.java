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


/**
 * This enum holds the different value serialization type encountered in an RDB file.
 *
 * @author John Whitbeck
 */
public enum ValueType {

  /**
   * A simple redis key/value pair as created by <code>set foo bar</code>.
   */
  VALUE,

  /**
   * A redis list as created by <code>lpush foo bar</code>.
   */
  LIST,

  /**
   *A redis set as created by <code>sadd foo bar</code>.
   */
  SET,

  /**
   * A redis sorted set as created by <code>zadd foo 1.2 bar</code>.
   */
  SORTED_SET,

  /**
   * A redis hash as created by <code>hset foo bar baz</code>.
   */
  HASH,

  /**
   * A compact encoding for small hashes. Deprecated as of redis 2.6.
   */
  ZIPMAP,

  /**
   * A compact encoding for small lists.
   */
  ZIPLIST,

  /**
   * A compact encoding for sets comprised entirely of integers.
   */
  INTSET,

  /**
   * A compact encoding for small sorted sets in which value/score pairs are flattened and stored in
   * a ZipList.
   */
  SORTED_SET_AS_ZIPLIST,

  /**
   * A compact encoding for small sorted sets in which value/score pairs are flattened and stored in
   * a ZipList.
   */
  SORTED_SET_AS_LISTPACK,

  /**
   * A compact encoding for small hashes in which key/value pairs are flattened and stored in a
   * ZipList.
   */
  HASHMAP_AS_ZIPLIST,

  /**
   * A linked list of ziplists to achieve good compression on lists of any length.
   */
  QUICKLIST,

  /**
   * A linked list of listpacks to achieve good compression on lists of any length.
   */
  QUICKLIST2,

  /**
   * Like SORTED_SET but encodes the scores as doubles using the IEEE 754 floating-point "double
   * format" bit layout on 8 bits instead of a string representation of the score.
   */
  SORTED_SET2,

  /**
   * A compact encoding for small hashes. Replaces HASHMAP_AS_ZIPLIST as of RDB 10.
   */
  HASHMAP_AS_LISTPACK,

  /**
   * A compact encoding of elements. Replaces ZIPLIST in RDB 10.
   */
  LISTPACK;
}
