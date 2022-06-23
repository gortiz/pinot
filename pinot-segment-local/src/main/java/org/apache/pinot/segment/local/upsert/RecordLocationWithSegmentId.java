package org.apache.pinot.segment.local.upsert;

import java.nio.ByteBuffer;

// Currently supports only Long as comparable since it is meant for POC purposes.
public class RecordLocationWithSegmentId {
  private final Integer _segmentId;
  private final int _docId;
  /** value used to denote the order */
  private final Comparable _comparisonValue;

  public RecordLocationWithSegmentId(Integer indexSegment, int docId, Comparable comparisonValue) {
    _segmentId = indexSegment;
    _docId = docId;
    _comparisonValue = comparisonValue;
  }

  public Integer getSegmentId() {
    return _segmentId;
  }

  public int getDocId() {
    return _docId;
  }

  public long getComparisonValue() {
    return (Long) _comparisonValue;
  }

  public byte[] asBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putInt(_segmentId);
    buffer.putInt(_docId);
    buffer.putLong((Long) _comparisonValue);
    return buffer.array();
  }

  public byte[] asBytes(ByteBuffer reuse) {
    reuse.putInt(0, _segmentId);
    reuse.putInt(Integer.BYTES, _docId);
    reuse.putLong(2*Integer.BYTES, (Long) _comparisonValue);
    return reuse.array();
  }
}
