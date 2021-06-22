# A simple Redis RDB file parser for Java

[![Build Status](https://travis-ci.org/jwhitbeck/java-rdb-parser.png)](https://travis-ci.org/jwhitbeck/java-rdb-parser.png)

## Overview

A simple Java library for parsing [Redis](http://redis.io) RDB files.

This library does the minimal amount of work to read entries (e.g. a new DB
selector, or a key/value pair with an expire time) from an RDB file, mostly
limiting itself to returning byte arrays or lists of byte arrays for keys and
values. The caller is responsible for application-level decisions such as how to
interpret the contents of the returned byte arrays or what types of objects to
instantiate from them.

For example, sorted sets and hashes are parsed as a flat list of value/score
pairs and key/value pairs, respectively. Simple Redis values are parsed as a
singleton. As expected, Redis lists and sets are parsed as lists of values.

Furthermore, this library performs lazy decoding of the packed encodings
(ZipMap, ZipList, Hashmap as ZipList, Sorted Set as ZipList, Intset, and
QuickList) such that those are only decoded when needed. This allows the caller
to efficiently skip over these entries or defer their decoding to a worker
thread.

RDB files created by all versions of Redis through 6.2.x are supported (i.e.,
RDB versions 1 through 9). Some features, however, are not supported:

- [Modules](https://redis.io/modules), introduced in RDB version 8
- [Streams](https://redis.io/topics/streams-intro), introduced in RDB version 9.

If you need these, please open an issue or a pull request.

To use this library, including the following dependency in your `pom.xml`.

```xml
<dependency>
    <groupId>net.whitbeck</groupId>
    <artifactId>rdb-parser</artifactId>
    <version>2.0.0</version>
</dependency>
```

Javadocs are available at
[javadoc.io/doc/net.whitbeck/rdb-parser/](http://www.javadoc.io/doc/net.whitbeck/rdb-parser/).

## Example usage

Let's begin by creating a new Redis RDB dump file.

Start a server in the background, connect a client to it, and flush all existing
data.

```
$ redis-server &
$ redis-cli
127.0.0.1:6379> flushall
```

Now let's create some data structures. Let's start with a simple key/value pair
with an expire time.

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

Finally, let's save the dump to disk. This will create a `dump.rdb` file in the
current directory.

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

  public static void printRdbFile(File file) throws Exception {
    try (RdbParser parser = new RdbParser(file)) {
      Entry e;
      while ((e = parser.readNext()) != null) {
        switch (e.getType()) {

        case SELECT_DB:
          System.out.println("Processing DB: " + ((SelectDb)e).getId());
          System.out.println("------------");
          break;

        case EOF:
          System.out.print("End of file. Checksum: ");
          for (byte b : ((Eof)e).getChecksum()) {
            System.out.print(String.format("%02x", b & 0xff));
          }
          System.out.println();
          System.out.println("------------");
          break;

        case KEY_VALUE_PAIR:
          System.out.println("Key value pair");
          KeyValuePair kvp = (KeyValuePair)e;
          System.out.println("Key: " + new String(kvp.getKey(), "ASCII"));
          Long expireTime = kvp.getExpiretime();
          if (expireTime != null) {
            System.out.println("Expire time (ms): " + expireTime);
          }
          System.out.println("Value type: " + kvp.getValueType());
          System.out.print("Values: ");
          for (byte[] val : kvp.getValues()) {
            System.out.print(new String(val, "ASCII") + " ");
          }
          System.out.println();
          System.out.println("------------");
          break;
        }
      }
    }
  }
}
```

Call this function on the `dump.rdb` file. The output will look like:

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
Expire time (ms): 1451518660934
Value type: VALUE
Values: bar
------------
End of file. Checksum: 157e40ad49ef13f6
------------
```

## References

As of June 2021, the most recent RDB format version is 9. The source of truth
is the [rdb.h][] file in the [Redis repo][]. The following resources provide a
good overview of the RDB format.

- [RDB file format](http://rdb.fnordig.de/file_format.html) (up to version 7).
- [RDB file format (redis-rdb-tools)](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format)
- [RDB version history (redis-rdb-tools)](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_Version_History.textile)

[rdb.h]: https://github.com/antirez/redis/blob/unstable/src/rdb.h
[Redis repo]: https://github.com/antirez/redis
