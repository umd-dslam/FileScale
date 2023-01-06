package org.apache.hadoop.hdfs.db;

public class DatabaseUtils {
  public static String getStackTrace() {
    String o = "Printing stack trace:\n";
    StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    for (int i = 1; i < elements.length; i++) {
      StackTraceElement s = elements[i];
      o +=
          "\tat "
              + s.getClassName()
              + "."
              + s.getMethodName()
              + "("
              + s.getFileName()
              + ":"
              + s.getLineNumber()
              + ")\n";
    }
    return o;
  }
}
