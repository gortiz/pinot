package org.apache.pinot.segment.local.upsert;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class UpsertPrimaryKeyLock {

  private UpsertPrimaryKeyLock(){}

  private static final int NUM_LOCKS = 10000;
  private static final Lock[] LOCKS = new Lock[NUM_LOCKS];

  static {
    for (int i = 0; i < NUM_LOCKS; i++) {
      LOCKS[i] = new ReentrantLock();
    }
  }

  public static Lock getPrimaryKeyLock(PrimaryKey primaryKey) {
    return LOCKS[Math.abs(primaryKey.hashCode() % NUM_LOCKS)];
  }
}
