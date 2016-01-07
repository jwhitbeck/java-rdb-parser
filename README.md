# A simple Redis RDB file parser for Java

[![Build Status](https://travis-ci.org/jwhitbeck/java-rdb-parser.png)](https://travis-ci.org/jwhitbeck/java-rdb-parser.png)

## Overview

A very simple Java library for parsing [Redis](http://redis.io) RDB files.

This library does the minimal amount of work to read entries (e.g. a new DB selector, or a key/value pair with
an expiry) from an RDB file. In particular, it does not make any assumptions about string encodings or the
types of objects to coerce Redis data into, thereby limiting itself to returning byte arrays or lists of byte
arrays for keys and values. Furthermore, it performs lazy decoding of the packed encodings (ZipList, Hashmap
as ZipList, Sorted Set as ZipList, and Intset) such that those are only decoded when needed.

The ZipMap encoding, deprecated as of redis 2.6, is not currently supported. If you need it, please open a
Github issue.

To use this library, including the following dependency in your `pom.xml`.

```xml
<dependency>
    <groupId>net.whitbeck</groupId>
    <artifactId>rdb-parser</artifactId>
    <version>1.0.0</version>
</dependency>
```

Javadocs are available at
[javadoc.io/doc/net.whitbeck/rdb-parser/](http://www.javadoc.io/doc/net.whitbeck/rdb-parser/).

## Example usage

Let's begin by creating a new Redis RDB dump file.

Start a server in the background, connect a client to it, a flush all existing data.

```
$ redis-server &
$ redis-cli
127.0.0.1:6379> flushall
```

Now let's create some data structures. Let's start with a simple key/value pair with an expiry.

```
127.0.0.1:6379> set foo bar
127.0.0.1:6379> expire foo 3600
```

Then let's create a small hash and a sorted set.

```
127.0.0.1:6379> hset myhash field1 val1
127.0.0.1:6379> hset myhash field2 val2
127.0.0.1:6379> zadd myset 1 one 2 two 2.5 two-point-five
```

Finally, let's save the dump to disk. This will create a `dump.rdb` file in the current directory.

```
127.0.0.1:6379> save
127.0.0.1:6379> exit
$ killall redis-server
```
Now let's see how to parse the `dump.rdb` file from Java.

```java
import java.io.File;
import net.whitbeck.rdbparser.*;

public class RdbFilePrinter {

  public void printRdbFile(File file) throws Exception {
    try (RdbParser parser = new RdbParser(file)) {
      Entry e;
      while ((e = parser.readNext()) != null) {
        switch (e.getType()) {

        case DB_SELECT:
          System.out.println("Processing DB: " + ((DbSelect)e).getId());
          break;

        case EOF:
          System.out.print("End of file. Checksum: ");
          for (byte b : ((Eof)e).getChecksum()) {
            System.out.print(String.format("%02x", b & 0xff));
          }
          System.out.println();
          break;

        case KEY_VALUE_PAIR:
          System.out.println("Key value pair");
          KeyValuePair kvp = (KeyValuePair)e;
          System.out.println("Key: " + new String(kvp.getKey(), "ASCII"));
          if (kvp.hasExpiry()) {
            System.out.println("Expiry (ms): " + kvp.getExpiryMillis());
          }
          System.out.println("Value type: " + kvp.getValueType());
          System.out.print("Values: ");
          for (byte[] val : kvp.getValues()) {
            System.out.print(new String(val, "ASCII") + " ");
          }
          System.out.println();
          break;
        }
        System.out.println("------------");
      }
    }
  }

}
```

Call this function on the `dump.rdb` file. The output will be:

```
Processing DB: 0
------------
Key value pair
Key: myset
Value type: SORTED_SET_AS_ZIPLIST
Values: one 1 two 2 two-point-five 2.5
------------
Key value pair
Key: myhash
Value type: HASHMAP_AS_ZIPLIST
Values: field1 val1 field2 val2
------------
Key value pair
Key: foo
Expiry (ms): 1451518660934
Value type: VALUE
Values: bar
------------
End of file. Checksum: 157e40ad49ef13f6
------------
```

Note that sorted sets and hashes are parsed as a flat list of value/score pairs and key/value pairs,
respectively. Simple redis values are parsed as a singleton. As expected, redis lists and sets are parsed as
lists of values.

## References

- [RDB file format](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format)
- [RDB file format history](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_Version_History.textile)
