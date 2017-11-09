package com.pingcap.tikv.exception;

public class HistogramException extends RuntimeException {
  public HistogramException(String msg) {
    super(msg);
  }
}