package com.inmobi.grill.server.query.rewrite;

public class ParseException extends Exception {

  public ParseException(String message) {
    super(message);
  }

  public ParseException(String message, Throwable e) {
    super(message, e);
  }

  public ParseException(Throwable th) {
    super(th);
  }
}
