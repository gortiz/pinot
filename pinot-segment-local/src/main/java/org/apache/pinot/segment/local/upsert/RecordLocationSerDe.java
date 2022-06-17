package org.apache.pinot.segment.local.upsert;

import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class RecordLocationSerDe {

  public static byte[] serialize(RecordLocationRef recordLocation) {
    try {
      return JsonUtils.objectToBytes(recordLocation);
    } catch (Exception e) {
      // log error
      return null;
    }
  }

  public static RecordLocationRef deserialize(byte[] recordLocationBytes) {
    try {
      return JsonUtils.bytesToObject(recordLocationBytes, RecordLocationRef.class);
    } catch (Exception e) {
      // log error
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    Schema schema = new Schema();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl("table", "segment", schema, System.currentTimeMillis());
    IndexSegment indexSegment = new EmptyIndexSegment(segmentMetadata);
    RecordLocationRef recordLocation = new RecordLocationRef(1, 1, System.currentTimeMillis());
    System.out.println(deserialize(serialize(recordLocation)));
  }
}
