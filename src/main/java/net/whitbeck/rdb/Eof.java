package net.whitbeck.rdb;

public class Eof extends Entry {

  private final byte[] checksum;

  Eof(byte[] checksum) {
    this.checksum = checksum;
  }

  @Override
  public int getType() {
    return Entry.EOF;
  }

  public byte[] getChecksum() {
    return checksum;
  }
}
