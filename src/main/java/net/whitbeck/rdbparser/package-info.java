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

/**
 * Provides a simple Redis RDB file parser for Java.
 *
 * <p>This library does the minimal amount of work to read entries (e.g. a new DB selector, or a
 * key/value pair with an expire time) from an RDB file, mostly limiting itself to returning byte
 * arrays or lists of byte arrays for keys and values. The caller is responsible for
 * application-level decisions like how to interpret the contents of the returned byte arrays or
 * what types of objects to instantiate from them.
 *
 * <p>For example, sorted sets and hashes are parsed as a flat list of value/score pairs and
 * key/value pairs, respectively. Simple Redis values are parsed as a singleton. As expected, Redis
 * lists and sets are parsed as lists of values.
 *
 * <p>Furthermore, this library performs lazy decoding of the packed encodings (ZipMap, ZipList,
 * Hashmap as ZipList, Sorted Set as ZipList, Intset, and Quicklist) such that those are only
 * decoded when needed. This allows the caller to efficiently skip over these entries or defer their
 * decoding to a worker thread.
 *
 * <p>RDB files created by all versions of Redis through 7.0.x are supported (i.e., RDB versions 1
 * through 10). Some features, however, are not supported:
 *
 * <ul>
 *   <li>Modules, introduced in RDB version 8</li>
 *   <li>Streams, introduced in RDB version 9.</li>
 * </ul>
 *
 * <p>If you need them, please open an issue or a pull request.
 *
 * <p>Implementation is not thread safe.
 *
 * <p>As of July 2023, the most recent RDB format version is 10. The source of truth is the <a
 * href="https://github.com/redis/redis/blob/unstable/src/rdb.h">rdb.h</a> file in the <a
 * href="https://github.com/redis/redis">Redis repo</a>. The following resources provide a good
 * overview of the RDB format.
 *
 * <ul>
 *   <li>
 *     <a href="http://rdb.fnordig.de/file_format.html">
 *      RDB file format
 *     </a> (up to version 7).
 *   </li>
 *   <li>
 *     <a href="https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format">
 *      RDB file format (redis-rdb-tools)
 *     </a>
 *   </li>
 *   <li>
 *     <a href="https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_Version_History.textile">
 *      RDB version history (redis-rdb-tools)
 *     </a>
 *   </li>
 * </ul>
 */
package net.whitbeck.rdbparser;
