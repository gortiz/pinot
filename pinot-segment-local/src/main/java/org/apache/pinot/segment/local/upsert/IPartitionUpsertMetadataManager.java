package org.apache.pinot.segment.local.upsert;

import java.util.Iterator;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.GenericRow;


public interface IPartitionUpsertMetadataManager {

  void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator);

  void addRecord(IndexSegment segment, RecordInfo recordInfo);

  GenericRow updateRecord(GenericRow record, RecordInfo recordInfo);

  void removeSegment(IndexSegment segment);

  void close();
}
