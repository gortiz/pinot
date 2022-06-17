package org.apache.pinot.segment.local.upsert;

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

  public Comparable getComparisonValue() {
    return _comparisonValue;
  }
}
