/**
 * Copyright (c) 2015-2021 John Whitbeck. All rights reserved.
 *
 * The use and distribution terms for this software are covered by the
 * Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0.txt)
 * which can be found in the file al-v20.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * You must not remove this notice, or any other, from this software.
 */

package net.whitbeck.rdbparser;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.rules.ExpectedException;

import redis.clients.jedis.Jedis;

@RunWith(Parameterized.class)
public class RdbParserTest {

  static final class RedisServerInstance {
    static int nextPort = 4000;

    final int rdbVersion;
    final String redisVersion;
    final int port;
    final File workDir;
    final File dumpFile;
    Process proc;
    Jedis jedis;

    RedisServerInstance(String redisVersion, int rdbVersion) {
      this.redisVersion = redisVersion;
      this.rdbVersion = rdbVersion;
      port = ++nextPort;
      workDir = new File("redis", "" + redisVersion);
      dumpFile = new File(workDir, "dump.rdb");
    }

    void startRedisServer() throws Exception {
      dumpFile.delete(); // start from an empty dump
      File configFile = new File(workDir, "src/configs/redis.conf");
      configFile.getParentFile().mkdirs();
      Files.write(configFile.toPath(), bytes("port " + port + "\n"));
      proc = new ProcessBuilder()
          .directory(workDir)
          .command("src/redis-server", configFile.getCanonicalPath())
          .start();
    }

    void startJedis() throws Exception {
      jedis = new Jedis("localhost", port);
    }

    void stop() throws Exception {
      jedis.close();
      proc.destroy();
      dumpFile.delete();
    }
  }

  static final RedisServerInstance[] instances = new RedisServerInstance[]{
    new RedisServerInstance("2.4.18", 6),
    new RedisServerInstance("2.8.24", 6),
    new RedisServerInstance("3.2.11", 7),
    new RedisServerInstance("4.0.6", 8),
    new RedisServerInstance("5.0.6", 9),
    new RedisServerInstance("6.2.1", 9)
  };

  @BeforeClass
  public static void startClients() throws Exception {
    for (RedisServerInstance inst : instances) {
      inst.startRedisServer();
    }
    Thread.sleep(2000); // wait for the redis servers to start
    for (RedisServerInstance inst : instances) {
      inst.startJedis();
    }
  }

  @AfterClass
  public static void stopClients() throws Exception {
    for (RedisServerInstance inst : instances) {
      inst.stop();
    }
  }

  int rdbVersion;
  String redisVersion;
  File dumpFile;
  Jedis jedis;

  @Parameters
  public static Collection<Object[]> params() {
    List<Object[]> params = new ArrayList<Object[]>(instances.length);
    for (RedisServerInstance inst : instances) {
      params.add(new Object[]{inst});
    }
    return params;
  }

  public RdbParserTest(RedisServerInstance inst) {
    this.rdbVersion = inst.rdbVersion;
    this.redisVersion = inst.redisVersion;
    this.dumpFile = inst.dumpFile;
    this.jedis = inst.jedis;
  }


  @Rule
  public ExpectedException thrown = ExpectedException.none();

  RdbParser openTestParser() throws IOException {
    return new RdbParser(dumpFile);
  }

  static String str(byte[] bs) throws Exception {
    return new String(bs, "ASCII");
  }

  static byte[] bytes(String s) throws Exception {
    return s.getBytes("ASCII");
  }

  static int compareVersions(String v1, String v2) {
    String[] elems1 = v1.split("\\.");
    String[] elems2 = v2.split("\\.");
    int major1 = Integer.parseInt(elems1[0]);
    int major2 = Integer.parseInt(elems2[0]);
    if (major1 > major2) {
      return 1;
    } else if (major1 < major2) {
      return -1;
    } else {
      int minor1 = Integer.parseInt(elems1[1]);
      int minor2 = Integer.parseInt(elems2[1]);
      if (minor1 > minor2) {
        return 1;
      } else if (minor1 < minor2) {
        return -1;
      } else {
        int bugfix1 = Integer.parseInt(elems1[2]);
        int bugfix2 = Integer.parseInt(elems2[2]);
        if (bugfix1 > bugfix2) {
          return 1;
        } else if (bugfix1 < bugfix2) {
          return -1;
        } else {
          return 0;
        }
      }
    }
  }

  static boolean isEarlierThan(String version, String cmp) {
    return compareVersions(version, cmp) < 0;
  }

  static boolean isLaterThan(String version, String cmp) {
    return compareVersions(version, cmp) > 0;
  }

  void setTestFile(ByteBuffer buf) throws IOException {
    try (FileChannel ch = FileChannel.open(dumpFile.toPath(),
                                           StandardOpenOption.WRITE,
                                           StandardOpenOption.CREATE,
                                           StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(buf);
    }
  }

  @Test
  public void magicNumber() throws Exception {
    setTestFile(ByteBuffer.wrap(bytes("not a valid redis file")));
    try (RdbParser p = openTestParser()) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Not a valid redis RDB file");
      p.readNext();
    }
  }

  @Test
  public void versionCheck() throws Exception {
    setTestFile(ByteBuffer.wrap(bytes("REDIS0042")));
    try (RdbParser p = openTestParser()) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Unknown version");
      p.readNext();
    }
  }

  @Test
  public void emptyFile() throws Exception {
    jedis.flushAll();
    jedis.save();
    try (RdbParser p = openTestParser()) {
      Entry t;
      AuxField aux;
      if (rdbVersion >= 7) {
        // AUX redis-ver = 3.2.0
        t = p.readNext();
        Assert.assertEquals(EntryType.AUX_FIELD, t.getType());
        aux = (AuxField)t;
        Assert.assertArrayEquals(bytes("redis-ver"), aux.getKey());
        Assert.assertArrayEquals(bytes(redisVersion), aux.getValue());
        // AUX redis-bits = 64
        t = p.readNext();
        Assert.assertEquals(EntryType.AUX_FIELD, t.getType());
        aux = (AuxField)t;
        Assert.assertArrayEquals(bytes("redis-bits"), aux.getKey());
        Assert.assertArrayEquals(bytes("64"), aux.getValue());
        // AUX ctime
        t = p.readNext();
        Assert.assertEquals(EntryType.AUX_FIELD, t.getType());
        aux = (AuxField)t;
        Assert.assertArrayEquals(bytes("ctime"), aux.getKey());
        // AUX used-mem = 821176
        t = p.readNext();
        Assert.assertEquals(EntryType.AUX_FIELD, t.getType());
        aux = (AuxField)t;
        Assert.assertArrayEquals(bytes("used-mem"), aux.getKey());
      }
      if (rdbVersion >= 8) {
        // AUX aof-preamble = 0
        t = p.readNext();
        Assert.assertEquals(EntryType.AUX_FIELD, t.getType());
        aux = (AuxField)t;
        Assert.assertArrayEquals(bytes("aof-preamble"), aux.getKey());
        Assert.assertArrayEquals(bytes("0"), aux.getValue());
      }
      // EOF
      t = p.readNext();
      Assert.assertEquals(EntryType.EOF, t.getType());
      // Nothing after EOF
      Assert.assertNull(p.readNext());
    }
  }

  void skipAuxFields(RdbParser p) throws IOException {
    // Skip the AUX entries at the beginning of the file.
    int numEntries = 0;
    switch (rdbVersion) {
      case 7:
        numEntries = 4;
        break;
      case 8:
      case 9:
        numEntries = 5;
        break;
    }
    for (int i=0; i<numEntries; i++) {
      p.readNext();
    }
  }

  @Test
  public void SelectDb() throws Exception {
    jedis.flushAll();
    jedis.select(1);
    jedis.set("foo", "bar");
    jedis.select(0);
    jedis.set("foo", "baz");
    jedis.save();
    try (RdbParser p = openTestParser()) {
      Entry t;
      ResizeDb resizeDb;
      SelectDb selectDb;
      KeyValuePair kvp;
      skipAuxFields(p);
      // DB SELECTOR 0
      t = p.readNext();
      Assert.assertEquals(EntryType.SELECT_DB, t.getType());
      selectDb = (SelectDb)t;
      Assert.assertEquals(0, selectDb.getId());
      if (rdbVersion >= 7) {
        // Resize DB
        t = p.readNext();
        Assert.assertEquals(EntryType.RESIZE_DB, t.getType());
        resizeDb = (ResizeDb)t;
        Assert.assertEquals(1, resizeDb.getDbHashTableSize());
        Assert.assertEquals(0, resizeDb.getExpireTimeHashTableSize());
      }
      // foo:bar
      t = p.readNext();
      Assert.assertEquals(EntryType.KEY_VALUE_PAIR, t.getType());
      kvp = (KeyValuePair)t;
      Assert.assertEquals(ValueType.VALUE, kvp.getValueType());
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("baz", str(kvp.getValues().get(0)));
      // DB SELECTOR 1
      t = p.readNext();
      Assert.assertEquals(EntryType.SELECT_DB, t.getType());
      selectDb = (SelectDb)t;
      Assert.assertEquals(1, selectDb.getId());
      if (rdbVersion >= 7) {
        // Resize DB
        t = p.readNext();
        Assert.assertEquals(EntryType.RESIZE_DB, t.getType());
        resizeDb = (ResizeDb)t;
        Assert.assertEquals(1, resizeDb.getDbHashTableSize());
        Assert.assertEquals(0, resizeDb.getExpireTimeHashTableSize());
      }
      // foo:baz
      t = p.readNext();
      Assert.assertEquals(EntryType.KEY_VALUE_PAIR, t.getType());
      kvp = (KeyValuePair)t;
      Assert.assertEquals(ValueType.VALUE, kvp.getValueType());
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("bar", str(kvp.getValues().get(0)));
      // EOF
      t = p.readNext();
      Assert.assertEquals(EntryType.EOF, t.getType());
    }
  }

  void skipToFirstKeyValuePair(RdbParser p) throws IOException {
    skipAuxFields(p); // Skip the AUX fields at top of file
    p.readNext(); // Skip the DB SELECTOR entry
    if (rdbVersion >= 7) {
      p.readNext(); // Skip the RESIZE_DB entry
    }
  }

  @Test
  public void expire() throws Exception {
    long expireTimeSecs = 3000000000L;
    long expireTimeMillis = 2000000000000L;
    int n = 0;
    jedis.flushAll();
    jedis.set("noexpiretime", "val");
    n++;
    jedis.set("seconds", "val");
    jedis.expireAt("seconds", expireTimeSecs);
    n++;
    if (!isEarlierThan(redisVersion, "2.6.0")) {
      jedis.set("millis", "val");
      jedis.pexpireAt("millis", expireTimeMillis);
      n++;
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      for (int i=0; i<n; ++i) {
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        String k = str(kvp.getKey());
        if (k.equals("noexpiretime")) {
          Assert.assertNull(kvp.getExpireTime());
        } else if (k.equals("seconds")) {
          Assert.assertEquals(expireTimeSecs * 1000L, kvp.getExpireTime().longValue());
        } else if (k.equals("millis")) {
          Assert.assertEquals(expireTimeMillis, kvp.getExpireTime().longValue());
        }
      }
    }
  }

  @Test
  public void stringRepresentations() throws Exception {
    jedis.flushAll();
    jedis.set("simple-key", "val");
    jedis.set("key-with-expire-time", "val");
    long expireTime = 2000000000L;
    jedis.expireAt("key-with-expire-time", expireTime);
    jedis.lpush("list-key", "one", "two", "three");
    jedis.set(new byte[]{0, 1, 2, 3}, bytes("val"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      if (rdbVersion >= 7) {
        // AUX entries
        Assert.assertEquals("AUX_FIELD (k: redis-ver, v: " + redisVersion + ")", p.readNext().toString());
        Assert.assertEquals("AUX_FIELD (k: redis-bits, v: 64)", p.readNext().toString());
        Assert.assertTrue(Pattern.matches("AUX_FIELD \\(k: ctime, v: \\d{10}\\)", p.readNext().toString()));
        Assert.assertTrue(Pattern.matches("AUX_FIELD \\(k: used-mem, v: \\d+\\)", p.readNext().toString()));
      }
      if (rdbVersion >= 8) {
        // more AUX fields
        Assert.assertEquals("AUX_FIELD (k: aof-preamble, v: 0)", p.readNext().toString());
      }
      // DB 0
      Assert.assertEquals("SELECT_DB (0)", p.readNext().toString());
      if (rdbVersion >= 7) {
        Assert.assertEquals("RESIZE_DB (db hash table size: 4, expire time hash table size: 1)",
                            p.readNext().toString());
      }
      for (int i=0; i<4; ++i) {
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        byte[] k = kvp.getKey();
        if (Arrays.equals(k, bytes("simple-key"))) {
          Assert.assertEquals("KEY_VALUE_PAIR (key: simple-key, 1 value)", kvp.toString());
        } else if (Arrays.equals(k, bytes("key-with-expire-time"))) {
          Assert.assertEquals("KEY_VALUE_PAIR (key: key-with-expire-time, expire time: "
                              + expireTime * 1000 + ", 1 value)",
                              kvp.toString());
        } else if (Arrays.equals(bytes("list-key"), k)) {
          Assert.assertEquals("KEY_VALUE_PAIR (key: list-key, 3 values)", kvp.toString());
        } else if (Arrays.equals(k, new byte[]{0, 1, 2, 3})) {
          Assert.assertEquals("KEY_VALUE_PAIR (key: \\x00\\x01\\x02\\x03, 1 value)", kvp.toString());
        }
      }
      Assert.assertTrue(Pattern.matches("EOF \\([\\da-f]{16}\\)", p.readNext().toString()));
    }
  }

  @Test
  public void binaryKeyAndValues() throws Exception {
    byte[] key = new byte[]{0, 1, 2, 3};
    byte[] val = new byte[]{4, 5, 6};
    jedis.flushAll();
    jedis.set(key, val);
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertArrayEquals(key, kvp.getKey());
      Assert.assertArrayEquals(val, kvp.getValues().get(0));
    }
  }

  @Test
  public void integerOneByteEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "12");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("12", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerOneByteEncodingSigned() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "-12");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("-12", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerTwoBytesEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "1234");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("1234", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerTwoBytesEncodingSigned() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "-1234");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("-1234", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerFourByteEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "123456789");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("123456789", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerFourByteEncodingSigned() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "-123456789");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("-123456789", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void lzfEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Assert.assertEquals("raw", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                          str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void list() throws Exception {
    jedis.flushAll();
    if (rdbVersion >= 7) {
      jedis.configSet("list-max-ziplist-size", "0");
    }
    jedis.lpush("foo", "bar", "1234", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.save();
    if (rdbVersion >= 7) {
      jedis.configSet("list-max-ziplist-size", "1000");
    }
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      if (rdbVersion < 7) {
        Assert.assertEquals(ValueType.ZIPLIST, kvp.getValueType());
      } else {
        Assert.assertEquals(ValueType.QUICKLIST, kvp.getValueType());
      }
      List<byte[]> list = kvp.getValues();
      Assert.assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", str(list.get(0)));
      Assert.assertEquals("1234", str(list.get(1)));
      Assert.assertEquals("bar", str(list.get(2)));
    }
  }

  @Test
  public void set() throws Exception {
    Set<String> set = new HashSet<String>();
    Collections.addAll(set, "bar", "1234", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.flushAll();
    for (String elem : set) {
      jedis.sadd("foo", elem);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals(ValueType.SET, kvp.getValueType());
      Set<String> parsedSet = new HashSet<String>();
      for (byte[] elem : kvp.getValues()) {
        parsedSet.add(str(elem));
      }
      Assert.assertEquals(set, parsedSet);
    }
  }

  @Test
  public void sortedSet() throws Exception {
    if (rdbVersion < 8) {
      Map<String, Double> valueScoreMap = new HashMap<String, Double>();
      valueScoreMap.put("foo", 1.45);
      valueScoreMap.put("bar", Double.POSITIVE_INFINITY);
      valueScoreMap.put("baz", Double.NEGATIVE_INFINITY);
      jedis.flushAll();
      String origValue = jedis.configGet("zset-max-ziplist-entries").get(1);
      jedis.configSet("zset-max-ziplist-entries", "0");
      for (Map.Entry<String, Double> e : valueScoreMap.entrySet()) {
        jedis.zadd("foo", e.getValue(), e.getKey());
      }
      jedis.save();
      jedis.configSet("zset-max-ziplist-entries", origValue);
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals(ValueType.SORTED_SET, kvp.getValueType());
        Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
        for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
          parsedValueScoreMap.put(str(i.next()), RdbParser.parseSortedSetScore(i.next()));
        }
        Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
      }
    }
  }

  @Test
  public void sortedSet2() throws Exception {
    if (rdbVersion >= 8) {
      Map<String, Double> valueScoreMap = new HashMap<String, Double>();
      valueScoreMap.put("foo", 1.45);
      valueScoreMap.put("bar", Double.POSITIVE_INFINITY);
      valueScoreMap.put("baz", Double.NEGATIVE_INFINITY);
      jedis.flushAll();
      String origValue = jedis.configGet("zset-max-ziplist-entries").get(1);
      jedis.configSet("zset-max-ziplist-entries", "0");
      for (Map.Entry<String, Double> e : valueScoreMap.entrySet()) {
        jedis.zadd("foo", e.getValue(), e.getKey());
      }
      jedis.save();
      jedis.configSet("zset-max-ziplist-entries", origValue);
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals(ValueType.SORTED_SET2, kvp.getValueType());
        Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
        for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
          parsedValueScoreMap.put(str(i.next()), RdbParser.parseSortedSet2Score(i.next()));
        }
        Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
      }
    }
  }

  @Test
  public void hash() throws Exception {
    String maxEntriesKey =
        isEarlierThan(redisVersion, "2.6.0")? "hash-max-zipmap-entries" : "hash-max-ziplist-entries";
    String origMaxEntriesValue = jedis.configGet(maxEntriesKey).get(1);
    jedis.configSet(maxEntriesKey, "0");
    Map<String, String> map = new HashMap<String, String>();
    map.put("one", "loremipsum");
    map.put("two", "2");
    map.put("three", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.flushAll();
    for (Map.Entry<String, String> e : map.entrySet()) {
      jedis.hset("foo", e.getKey(), e.getValue());
    }
    jedis.save();
    jedis.configSet(maxEntriesKey, origMaxEntriesValue);
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals(ValueType.HASH, kvp.getValueType());
      Map<String,String> parsedMap = new HashMap<String,String>();
      for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
        parsedMap.put(str(i.next()), str(i.next()));
      }
      Assert.assertEquals(map, parsedMap);
    }
  }

  @Test
  public void quickList() throws Exception {
    if (rdbVersion >= 7) {
      List<String> list = Arrays.asList("loremipsum", // string
                                        "10", // 4 bit integer
                                        "30", // 8 bit integer
                                        "-30", // 8 bit signed integer
                                        "1000", // 16 bit integer
                                        "-1000", // 16 bit signed integer
                                        "300000", // 24 bit integer
                                        "-300000", // 24 bit signed integer
                                        "30000000", // 32 bit integer
                                        "-30000000", // 32 bit signed integer
                                        "9000000000", // 64 bit integer
                                        "-9000000000" // 64 bit signed integer
        );
      jedis.flushAll();
      for (String s : list) {
        jedis.lpush("foo", s);
      }
      jedis.save();
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals(ValueType.QUICKLIST, kvp.getValueType());
        List<String> parsedList = new ArrayList<String>();
        for (byte[] val : kvp.getValues()) {
          parsedList.add(str(val));
        }
        Collections.reverse(parsedList);
        Assert.assertEquals(list, parsedList);
      }
    }
  }

  @Test
  public void intSet16Bit() throws Exception {
    Set<String> ints = new HashSet<String>();
    Collections.addAll(ints, "1", "-1", "12", "-12");
    jedis.flushAll();
    for (String s : ints) {
      jedis.sadd("foo", s);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals(ValueType.INTSET, kvp.getValueType());
      Set<String> parsedInts = new HashSet<String>();
      for (byte[] bs : kvp.getValues()) {
        parsedInts.add(str(bs));
      }
      Assert.assertEquals(ints, parsedInts);
    }
  }

  @Test
  public void intSet32Bit() throws Exception {
    Set<String> ints = new HashSet<String>();
    Collections.addAll(ints, "1", "-1", "30000000", "-30000000");
    jedis.flushAll();
    for (String s : ints) {
      jedis.sadd("foo", s);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals(ValueType.INTSET, kvp.getValueType());
      Set<String> parsedInts = new HashSet<String>();
      for (byte[] bs : kvp.getValues()) {
        parsedInts.add(str(bs));
      }
      Assert.assertEquals(ints, parsedInts);
    }
  }

  @Test
  public void intSet64Bit() throws Exception {
    Set<String> ints = new HashSet<String>();
    Collections.addAll(ints, "1", "-1", "9000000000", "-9000000000");
    jedis.flushAll();
    for (String s : ints) {
      jedis.sadd("foo", s);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals(ValueType.INTSET, kvp.getValueType());
      Set<String> parsedInts = new HashSet<String>();
      for (byte[] bs : kvp.getValues()) {
        parsedInts.add(str(bs));
      }
      Assert.assertEquals(ints, parsedInts);
    }
  }

  @Test
  public void sortedSetAsZipList() throws Exception {
    Map<String, Double> valueScoreMap = new HashMap<String, Double>();
    valueScoreMap.put("foo", 1.45);
    valueScoreMap.put("bar", Double.POSITIVE_INFINITY);
    valueScoreMap.put("baz", Double.NEGATIVE_INFINITY);
    jedis.flushAll();
    for (Map.Entry<String, Double> e : valueScoreMap.entrySet()) {
      jedis.zadd("foo", e.getValue(), e.getKey());
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      skipToFirstKeyValuePair(p);
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals(ValueType.SORTED_SET_AS_ZIPLIST, kvp.getValueType());
      Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
      for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ){
        parsedValueScoreMap.put(str(i.next()), RdbParser.parseSortedSetScore(i.next()));
      }
      Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
    }
  }

  @Test
  public void hashmapAsZipList() throws Exception {
    if (isLaterThan(redisVersion, "2.6.0")) {
      StringBuffer sb = new StringBuffer(16384);
      for (int i=0; i<64; ++i) {
        sb.append("x");
      }
      String mediumString = sb.toString();
      for (int i=0; i<256; ++i) {
        sb.append(mediumString);
      }
      String longString = sb.toString();
      Map<String, String> map = new HashMap<String, String>();
      map.put("one", "loremipsum");
      map.put("two", "2");
      map.put("three", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
      map.put("four", mediumString);
      map.put("five", longString);
      jedis.configSet("hash-max-ziplist-value", Integer.toString(longString.length()));
      jedis.flushAll();
      for (Map.Entry<String, String> e : map.entrySet()) {
        jedis.hset("foo", e.getKey(), e.getValue());
      }
      jedis.save();
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals(ValueType.HASHMAP_AS_ZIPLIST, kvp.getValueType());
        Map<String,String> parsedMap = new HashMap<String,String>();
        for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
          parsedMap.put(str(i.next()), str(i.next()));
        }
        Assert.assertEquals(map, parsedMap);
      }
    }
  }

  @Test
  public void zipMap() throws Exception {
    if (isEarlierThan(redisVersion, "2.6.0")) {
      Map<String, String> map = new HashMap<String, String>();
      map.put("one", "loremipsum");
      map.put("two", "2");
      map.put("three", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
      jedis.flushAll();
      for (Map.Entry<String, String> e : map.entrySet()) {
        jedis.hset("foo", e.getKey(), e.getValue());
      }
      jedis.save();
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals(ValueType.ZIPMAP, kvp.getValueType());
        Map<String,String> parsedMap = new HashMap<String,String>();
        for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
          parsedMap.put(str(i.next()), str(i.next()));
        }
        Assert.assertEquals(map, parsedMap);
      }
    }
  }

  @Test
  public void memoryPolicyLRU() throws Exception {
    if(rdbVersion >= 9) {
      jedis.flushAll();
      jedis.configSet("maxmemory-policy", "allkeys-lru");
      jedis.set("foo", "bar");
      jedis.save();
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals("foo", str(kvp.getKey()));
        Assert.assertEquals("bar", str(kvp.getValues().get(0)));
        Assert.assertEquals(0L, (long)kvp.getIdle());
      }
      jedis.configSet("maxmemory-policy", "noeviction");
    }
  }

  @Test
  public void memoryPolicyLFU() throws Exception {
    if(rdbVersion >= 9) {
      jedis.flushAll();
      jedis.configSet("maxmemory-policy", "allkeys-lfu");
      jedis.set("foo", "bar");
      jedis.save();
      try (RdbParser p = openTestParser()) {
        skipToFirstKeyValuePair(p);
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        Assert.assertEquals("foo", str(kvp.getKey()));
        Assert.assertEquals("bar", str(kvp.getValues().get(0)));
        Assert.assertEquals(5L, (long)kvp.getFreq());
      }
      jedis.configSet("maxmemory-policy", "noeviction");
    }
  }

}
