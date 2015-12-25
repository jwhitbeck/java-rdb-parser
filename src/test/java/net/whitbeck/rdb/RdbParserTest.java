package net.whitbeck.rdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    setTestFile(ByteBuffer.wrap("not a valid redis file".getBytes()));
    try (RdbParser p = openTestParser()) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Not a valid redis RDB file");
      p.readNext();
    }
  }

  @Test
  public void versionCheck() throws IOException {
    setTestFile(ByteBuffer.wrap("REDIS0042".getBytes()));
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
      Assert.assertTrue(t.getType() == Entry.EOF);
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
      Assert.assertEquals(Entry.DB_SELECTOR, t.getType());
      DbSelect dbSelect = (DbSelect)t;
      Assert.assertEquals(0, dbSelect.getId());
      // foo:bar
      t = p.readNext();
      Assert.assertEquals(Entry.KEY_VALUE_PAIR, t.getType());
      KeyValue v = (KeyValue)t;
      Assert.assertEquals(KeyValuePair.VALUE, v.getValueType());
      Assert.assertEquals("foo", str(v.getKey()));
      Assert.assertEquals("baz", str(v.getValue()));
      // DB_SELECTOR 1
      t = p.readNext();
      Assert.assertTrue(t.getType() == Entry.DB_SELECTOR);
      dbSelect = (DbSelect)t;
      Assert.assertEquals(1, dbSelect.getId());
      // foo:baz
      t = p.readNext();
      Assert.assertEquals(Entry.KEY_VALUE_PAIR, t.getType());
      v = (KeyValue)t;
      Assert.assertEquals(KeyValuePair.VALUE, v.getValueType());
      Assert.assertEquals("foo", str(v.getKey()));
      Assert.assertEquals("bar", str(v.getValue()));
      // EOF
      t = p.readNext();
      Assert.assertTrue(t.getType() == Entry.EOF);
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
      KeyValue se = (KeyValue)(p.readNext());
      Assert.assertArrayEquals(key, se.getKey());
      Assert.assertArrayEquals(val, se.getValue());
    }
  }

  @Test
  public void integerOneByteEncodingTest() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "12");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValue se = (KeyValue)(p.readNext());
      Assert.assertEquals("foo", str(se.getKey()));
      Assert.assertEquals("12", str(se.getValue()));
    }
  }

  @Test
  public void integerTwoBytesEncodingTest() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "1234");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValue se = (KeyValue)(p.readNext());
      Assert.assertEquals("foo", str(se.getKey()));
      Assert.assertEquals("1234", str(se.getValue()));
    }
  }

  @Test
  public void integerFourByteEncodingTest() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "123456789");
    Assert.assertEquals("int", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValue se = (KeyValue)(p.readNext());
      Assert.assertEquals("foo", str(se.getKey()));
      Assert.assertEquals("123456789", str(se.getValue()));
    }
  }

  @Test
  public void LzfEncodingTest() throws Exception {
    jedis.flushAll();
    jedis.set("foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Assert.assertEquals("raw", jedis.objectEncoding("foo"));
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValue se = (KeyValue)(p.readNext());
      Assert.assertEquals("foo", str(se.getKey()));
      Assert.assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                          str(se.getValue()));
    }
  }

  @Test
  public void listTest() throws Exception {
    jedis.flushAll();
    String origValue = jedis.configGet("list-max-ziplist-entries").get(1);
    jedis.configSet("list-max-ziplist-entries", "0");
    jedis.lpush("foo", "bar", "1234", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.save();
    jedis.configSet("list-max-ziplist-entries", origValue);
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.LIST == vs.getValueType());
      byte[][] list = vs.getValues();
      Assert.assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", str(list[0]));
      Assert.assertEquals("1234", str(list[1]));
      Assert.assertEquals("bar", str(list[2]));
    }
  }

  @Test
  public void setTest() throws Exception {
    Set<String> set = new HashSet<String>();
    Collections.addAll(set, "bar", "1234", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    jedis.flushAll();
    for (String elem : set) {
      jedis.sadd("foo", elem);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.SET == vs.getValueType());
      Set<String> parsedSet = new HashSet<String>();
      for (byte[] elem : vs.getValues()) {
        parsedSet.add(str(elem));
      }
      Assert.assertEquals(set, parsedSet);
    }
  }

  @Test
  public void sortedSetTest() throws Exception {
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
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.SORTED_SET == vs.getValueType());
      byte[][] valueScoresPairs = vs.getValues();
      Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
      for (int i=0; i<valueScoresPairs.length; i+= 2) {
        parsedValueScoreMap.put(str(valueScoresPairs[i]),
                                Double.parseDouble(str(valueScoresPairs[i+1])));
      }
      Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
    }
  }

  @Test
  public void hashTest() throws Exception {
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
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.HASH == vs.getValueType());
      byte[][] kvPairs = vs.getValues();
      Map<String,String> parsedMap = new HashMap<String,String>();
      for (int i=0; i<kvPairs.length; i+=2) {
        parsedMap.put(str(kvPairs[i]), str(kvPairs[i+1]));
      }
      Assert.assertEquals(map, parsedMap);
    }
  }

  @Test
  public void zipListTest() throws Exception {
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
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.ZIPLIST == vs.getValueType());
      List<String> parsedList = new ArrayList<String>();
      for (byte[] val : vs.getValues()) {
        parsedList.add(str(val));
      }
      Collections.reverse(parsedList);
      Assert.assertEquals(list, parsedList);
    }
  }

  @Test
  public void intSet16BitTest() throws Exception {
    Set<String> ints = new HashSet<String>();
    Collections.addAll(ints, "1", "-1", "12", "-12");
    jedis.flushAll();
    for (String s : ints) {
      jedis.sadd("foo", s);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.INTSET == vs.getValueType());
      Set<String> parsedInts = new HashSet<String>();
      for (byte[] bs : vs.getValues()) {
        parsedInts.add(str(bs));
      }
      Assert.assertEquals(ints, parsedInts);
    }
  }

  @Test
  public void intSet32BitTest() throws Exception {
    Set<String> ints = new HashSet<String>();
    Collections.addAll(ints, "1", "-1", "30000000", "-30000000");
    jedis.flushAll();
    for (String s : ints) {
      jedis.sadd("foo", s);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.INTSET == vs.getValueType());
      Set<String> parsedInts = new HashSet<String>();
      for (byte[] bs : vs.getValues()) {
        parsedInts.add(str(bs));
      }
      Assert.assertEquals(ints, parsedInts);
    }
  }

  @Test
  public void intSet64BitTest() throws Exception {
    Set<String> ints = new HashSet<String>();
    Collections.addAll(ints, "1", "-1", "9000000000", "-9000000000");
    jedis.flushAll();
    for (String s : ints) {
      jedis.sadd("foo", s);
    }
    jedis.save();
    try (RdbParser p = openTestParser()) {
      p.readNext(); // skip DB_SELECTOR
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.INTSET == vs.getValueType());
      Set<String> parsedInts = new HashSet<String>();
      for (byte[] bs : vs.getValues()) {
        parsedInts.add(str(bs));
      }
      Assert.assertEquals(ints, parsedInts);
    }
  }

  @Test
  public void sortedSetAsZipListTest() throws Exception {
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
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.SORTED_SET_AS_ZIPLIST == vs.getValueType());
      byte[][] valueScoresPairs = vs.getValues();
      Map<String, Double> parsedValueScoreMap = new HashMap<String, Double>();
      for (int i=0; i<valueScoresPairs.length; i+=2) {
        parsedValueScoreMap.put(str(valueScoresPairs[i]),
                                Double.parseDouble(str(valueScoresPairs[i+1])));
      }
      Assert.assertEquals(valueScoreMap, parsedValueScoreMap);
    }
  }

  @Test
  public void hashmapAsZipListTest() throws Exception {
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
      KeyValues vs = (KeyValues)(p.readNext());
      Assert.assertTrue(KeyValuePair.HASHMAP_AS_ZIPLIST == vs.getValueType());
      byte[][] kvPairs = vs.getValues();
      Map<String,String> parsedMap = new HashMap<String,String>();
      for (int i=0; i<kvPairs.length; i+=2) {
        parsedMap.put(str(kvPairs[i]), str(kvPairs[i+1]));
      }
      Assert.assertEquals(map, parsedMap);
    }
  }

}
