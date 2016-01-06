/**
 * Copyright (c) 2015-2016 John Whitbeck. All rights reserved.
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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.rules.ExpectedException;

import redis.clients.jedis.Jedis;

public class RdbParserTest {

  static final File tmpFile = new File("dump.rdb");
  static final int REDIS_PORT = 4444;
  static Process redisServerProc;
  static Jedis jedis;

  void setTestFile(ByteBuffer buf) throws IOException {
    try (FileChannel ch = FileChannel.open(tmpFile.toPath(),
                                           StandardOpenOption.WRITE,
                                           StandardOpenOption.CREATE,
                                           StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(buf);
    }
  }

  @BeforeClass
  public static void ensureRedisServerIsUp() throws Exception {
    if (redisServerProc == null) {
      tmpFile.delete(); // start from an empty dump
      redisServerProc = new ProcessBuilder("redis-server", "--port", "" + REDIS_PORT).start();
      Thread.sleep(2000); // wait for the redis server to start
    }
  }

  @BeforeClass
  public static void startJedisClient() {
    jedis = new Jedis("localhost", REDIS_PORT);
  }

  @AfterClass
  public static void ensureRedisServerIsDown() throws IOException {
    if (redisServerProc != null) {
      redisServerProc.destroy();
    }
  }

  @AfterClass
  public static void closeJedisClient() {
    jedis.close();
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  RdbParser openTestParser() throws IOException {
    return new RdbParser(tmpFile);
  }

  String str(byte[] bs) throws Exception {
    return new String(bs, "ASCII");
  }

  @Test
  public void magicNumber() throws IOException {
    setTestFile(ByteBuffer.wrap("not a valid redis file".getBytes("ASCII")));
    try (RdbParser p = openTestParser()) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Not a valid redis RDB file");
      p.readNext();
    }
  }

  @Test
  public void versionCheck() throws IOException {
    setTestFile(ByteBuffer.wrap("REDIS0042".getBytes("ASCII")));
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
      Entry t = p.readNext();
      Assert.assertTrue(t.getType() == EntryType.EOF);
      Eof eof = (Eof)t;
      Assert.assertArrayEquals(new byte[]{(byte)0xdc, (byte)0xb3, (byte)0x43, (byte)0xf0,
                                          (byte)0x5a, (byte)0xdc, (byte)0xf2, (byte)0x56},
                               eof.getChecksum());
      Assert.assertNull(p.readNext());
    }
  }

  @Test
  public void dbSelect() throws Exception {
    jedis.flushAll();
    jedis.select(1);
    jedis.set("foo", "bar");
    jedis.select(0);
    jedis.set("foo", "baz");
    jedis.save();
    try (RdbParser p = openTestParser()) {
      // DB_SELECTOR 0
      Entry t = p.readNext();
      Assert.assertEquals(EntryType.DB_SELECT, t.getType());
      DbSelect dbSelect = (DbSelect)t;
      Assert.assertEquals(0, dbSelect.getId());
      // foo:bar
      t = p.readNext();
      Assert.assertEquals(EntryType.KEY_VALUE_PAIR, t.getType());
      KeyValuePair kvp = (KeyValuePair)t;
      Assert.assertEquals(ValueType.VALUE, kvp.getValueType());
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("baz", str(kvp.getValues().get(0)));
      // DB_SELECTOR 1
      t = p.readNext();
      Assert.assertTrue(t.getType() == EntryType.DB_SELECT);
      dbSelect = (DbSelect)t;
      Assert.assertEquals(1, dbSelect.getId());
      // foo:baz
      t = p.readNext();
      Assert.assertEquals(EntryType.KEY_VALUE_PAIR, t.getType());
      kvp = (KeyValuePair)t;
      Assert.assertEquals(ValueType.VALUE, kvp.getValueType());
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("bar", str(kvp.getValues().get(0)));
      // EOF
      t = p.readNext();
      Assert.assertTrue(t.getType() == EntryType.EOF);
    }
  }

  @Test
  public void expiries() throws Exception {
    long curTimeMills = System.currentTimeMillis();
    jedis.flushAll();
    jedis.set("noexpiry", "val");
    jedis.set("seconds", "val");
    jedis.expire("seconds", 60);
    jedis.set("millis", "val");
    jedis.pexpire("millis", 10000L);
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      for (int i=0; i<3; ++i) {
        KeyValuePair kvp = (KeyValuePair)(p.readNext());
        String k = new String(kvp.getKey(), "ASCII");
        if (k.equals("noexpiry")) {
          Assert.assertFalse(kvp.hasExpiry());
        } else if (k.equals("seconds")) {
          Assert.assertTrue(kvp.hasExpiry());
          // Tolerate a second between the rdb dump and the test time
          Assert.assertTrue(kvp.getExpiryMillis() > curTimeMills + 59000);
        } else if (k.equals("millis")) {
          Assert.assertTrue(kvp.hasExpiry());
          // Tolerate a second between the rdb dump and the test time
          Assert.assertTrue(kvp.getExpiryMillis() > curTimeMills + 9000);
        }
      }
    }
  }

  @Test
  public void stringRepresentations() throws Exception {
    jedis.flushAll();
    jedis.set("simple-key", "val");
    jedis.set("key-with-expiry", "val");
    long expiry = 2000000000000L;
    jedis.pexpireAt("key-with-expiry", expiry);
    jedis.lpush("list-key", "one", "two", "three");
    jedis.set(new byte[]{0, 1, 2, 3}, "val".getBytes("ASCII"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      Assert.assertEquals(p.readNext().toString(), "DB_SELECT (0)");
      for (int i=0; i<4; ++i) {
        KeyValuePair kvp = (KeyValuePair)p.readNext();
        byte[] k = kvp.getKey();
        if (Arrays.equals(k, "simple-key".getBytes("ASCII"))) {
          Assert.assertEquals(kvp.toString(), "KEY_VALUE_PAIR (key: simple-key, 1 value)");
        } else if (Arrays.equals(k, "key-with-expiry".getBytes("ASCII"))) {
          Assert.assertEquals(kvp.toString(),
                              "KEY_VALUE_PAIR (key: key-with-expiry, expiry: " + expiry+ ", 1 value)");
        } else if (Arrays.equals(k, "list-key".getBytes("ASCII"))) {
          Assert.assertEquals(kvp.toString(), "KEY_VALUE_PAIR (key: list-key, 3 values)");
        } else if (Arrays.equals(k, new byte[]{0, 1, 2, 3})) {
          Assert.assertEquals(kvp.toString(), "KEY_VALUE_PAIR (key: \\x00\\x01\\x02\\x03, 1 value)");
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
      p.readNext(); // skip DB_SELECTOR
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("12", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerTwoBytesEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "1234");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("1234", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void integerFourByteEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "123456789");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("123456789", str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void lzfEncoding() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Assert.assertEquals("raw", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertEquals("foo", str(kvp.getKey()));
      Assert.assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                          str(kvp.getValues().get(0)));
    }
  }

  @Test
  public void list() throws Exception {
    jedis.flushAll();
    String origValue = jedis.configGet("list-max-ziplist-entries").get(1);
    jedis.configSet("list-max-ziplist-entries", "0");
    jedis.lpush("foo", "bar", "1234", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.save();
    jedis.configSet("list-max-ziplist-entries", origValue);
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.LIST == kvp.getValueType());
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.SET == kvp.getValueType());
      Set<String> parsedSet = new HashSet<String>();
      for (byte[] elem : kvp.getValues()) {
        parsedSet.add(str(elem));
      }
      Assert.assertEquals(set, parsedSet);
    }
  }

  @Test
  public void sortedSet() throws Exception {
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.SORTED_SET == kvp.getValueType());
      Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
      for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
        parsedValueScoreMap.put(str(i.next()), Double.parseDouble(str(i.next())));
      }
      Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
    }
  }

  @Test
  public void hash() throws Exception {
    Map<String, String> map = new HashMap<String, String>();
    map.put("one", "loremipsum");
    map.put("two", "2");
    map.put("three", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.flushAll();
    String origValue = jedis.configGet("hash-max-ziplist-entries").get(1);
    jedis.configSet("hash-max-ziplist-entries", "0");
    for (Map.Entry<String, String> e : map.entrySet()) {
      jedis.hset("foo", e.getKey(), e.getValue());
    }
    jedis.save();
    jedis.configSet("hash-max-ziplist-entries", origValue);
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.HASH == kvp.getValueType());
      Map<String,String> parsedMap = new HashMap<String,String>();
      for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
        parsedMap.put(str(i.next()), str(i.next()));
      }
      Assert.assertEquals(map, parsedMap);
    }
  }

  @Test
  public void zipList() throws Exception {
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.ZIPLIST == kvp.getValueType());
      List<String> parsedList = new ArrayList<String>();
      for (byte[] val : kvp.getValues()) {
        parsedList.add(str(val));
      }
      Collections.reverse(parsedList);
      Assert.assertEquals(list, parsedList);
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.INTSET == kvp.getValueType());
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.INTSET == kvp.getValueType());
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.INTSET == kvp.getValueType());
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.SORTED_SET_AS_ZIPLIST == kvp.getValueType());
      Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
      for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ){
        parsedValueScoreMap.put(str(i.next()), Double.parseDouble(str(i.next())));
      }
      Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
    }
  }

  @Test
  public void hashmapAsZipList() throws Exception {
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
      p.readNext(); // skip DB_SELECTOR
      KeyValuePair kvp = (KeyValuePair)p.readNext();
      Assert.assertTrue(ValueType.HASHMAP_AS_ZIPLIST == kvp.getValueType());
      Map<String,String> parsedMap = new HashMap<String,String>();
      for (Iterator<byte[]> i = kvp.getValues().iterator(); i.hasNext(); ) {
        parsedMap.put(str(i.next()), str(i.next()));
      }
      Assert.assertEquals(map, parsedMap);
    }
  }
}
