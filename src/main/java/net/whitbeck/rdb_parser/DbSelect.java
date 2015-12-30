package net.whitbeck.rdb_parser;

public class DbSelect extends Entry {

  private final long id;

  DbSelect(long id) {
    this.id = id;
  }

  @Override
  public int getType() {
    return Entry.DB_SELECTOR;
  }

  public long getId() {
    return id;
  }
}
