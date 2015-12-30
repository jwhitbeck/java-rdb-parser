package net.whitbeck.rdb_parser;

public abstract class Entry {

  public final static int
    EOF = 0,
    DB_SELECTOR = 1,
    KEY_VALUE_PAIR = 2;

  public abstract int getType();
}
