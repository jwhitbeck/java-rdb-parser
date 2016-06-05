/**
 * Copyright (c) 2015-2016 John Whitbeck. All rights reserved.
 *
 * <p>The use and distribution terms for this software are covered by the
 * Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0.txt)
 * which can be found in the file al-v20.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * <p>You must not remove this notice, or any other, from this software.
 */

/**
 * Provides a simple Redis RDB file parser for Java.
 *
 * <p>This library does the minimal amount of work to read entries (e.g. a new DB selector, or a
 * key/value pair with an expiry) from an RDB file. In particular, it does not make any assumptions
 * about string encodings or the types of objects to coerce Redis data into, thereby limiting itself
 * to returning byte arrays or lists of byte arrays for keys and values. Furthermore, it performs
 * lazy decoding of the packed encodings (ZipList, Hashmap as ZipList, Sorted Set as ZipList, and
 * Intset) such that those are only decoded when needed.
 *
 * <p>The ZipMap encoding, deprecated as of Redis 2.6, is not currently supported. If you need it,
 * please open a Github issue.
 *
 * <p>Implemenation is not thread safe.
 *
 * <p>References:
 * <ul>
 *   <li>
 *     <a href="http://rdb.fnordig.de/file_format.html">
 *      RDB file format
 *     </a>
 *   </li>
 * </ul>
*/
package net.whitbeck.rdbparser;
