package org.apache.pinot.segment.local.upsert;

import java.nio.ByteBuffer;
import org.apache.pinot.segment.spi.IndexSegment;


public class RecordLocationRef {
  private final Integer _segmentRef;
  private final int _docId;
  /** value used to denote the order */
  private final Comparable _comparisonValue;

  public RecordLocationRef(Integer indexSegment, int docId, Comparable comparisonValue) {
    _segmentRef = indexSegment;
    _docId = docId;
    _comparisonValue = comparisonValue;
  }

  public Integer getSegmentRef() {
    return _segmentRef;
  }

  public int getDocId() {
    return _docId;
  }

  public long getComparisonValue() {
    return (Long) _comparisonValue;
  }

  public byte[] asBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putInt(_segmentRef);
    buffer.putInt(_docId);
    buffer.putLong((Long) _comparisonValue);
    return buffer.array();
  }

}
