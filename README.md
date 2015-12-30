# A simple Redis RDB file parser for Java

[![Build Status](https://travis-ci.org/jwhitbeck/java-rdb-parser.png)](https://travis-ci.org/jwhitbeck/java-rdb-parser.png)

## Overview

A very simple Java library for parsing [Redis](http://redis.io)
[RDB](https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format) files.

This library does the minimal amount of work to read entries (e.g. a new DB selector, or a key/value pair with
an expiry) from an RDB file. In particular, it does not make any assumptions about string encodings or the
types of objects to coerce Redis data into, thereby limiting itself to returning byte arrays or lists of byte
arrays for keys and values. Furthermore, it performs lazy decoding of the packed encodings (ZipList, Hashmap
as ZipList, Sorted Set as ZipList, and Intset) such that those are only decoded when needed.

The ZipMap encoding, deprecated as of RDB
[version 4](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_Version_History.textile),
is not currently supported. If you need it, please open a Github issue.

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

TODO: maven instructions

```java
import java.io.File;
import net.whitbeck.rdb_parser.*;

public class RDBFilePrinter {

  public void printRDBFile(File file) throws Exception {
    try (RdbParser parser = new RdbParser(file)) {
      Entry e;
      while ((e = parser.readNext()) != null) {
        switch (e.getType()) {

        case Entry.DB_SELECT:
          System.out.println("Parsing DB: " + ((DbSelect)e).getId());
          break;

        case Entry.EOF:
          System.out.print("End of file. Checksum: ");
          for (byte b : ((Eof)e).getChecksum()) {
            System.out.print(String.format("%02x", b & 0xff));
          }
          System.out.println();
          break;

        case Entry.KEY_VALUE_PAIR:
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
Parsing DB: 0
------------
Key value pair
Key: myset
Value type: 12
Values: one 1 two 2 two-point-five 2.5
------------
Key value pair
Key: myhash
Value type: 13
Values: field1 val1 field2 val2
------------
Key value pair
Key: foo
Expiry (ms): 1451518660934
Value type: 0
Values: bar
------------
End of file. Checksum: 157e40ad49ef13f6
------------
```
