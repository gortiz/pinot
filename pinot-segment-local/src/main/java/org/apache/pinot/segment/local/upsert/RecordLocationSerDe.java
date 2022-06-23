package org.apache.pinot.segment.local.upsert;

import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.data.Schema;

//TODO: Make ser/de faster
public class RecordLocationSerDe {

  public static byte[] serialize(RecordLocationWithSegmentId recordLocation) {
    try {
      return recordLocation.asBytes();
    } catch (Exception e) {
      // log error
      return null;
    }
  }

  public static byte[] serialize(RecordLocationWithSegmentId recordLocation, ByteBuffer reuse) {
    try {
      return recordLocation.asBytes(reuse);
    } catch (Exception e) {
      // log error
      return null;
    }
  }

  public static RecordLocationWithSegmentId deserialize(byte[] recordLocationBytes) {
    try {
      ByteBuffer byteBuffer = ByteBuffer.wrap(recordLocationBytes);
      int segmentRef = byteBuffer.getInt(0);
      if(segmentRef == -1) return null;

      int docId = byteBuffer.getInt(Integer.BYTES);
      long comparisonValue = byteBuffer.getLong(2* Integer.BYTES);

      return new RecordLocationWithSegmentId(segmentRef, docId, comparisonValue);
    } catch (Exception e) {
      // log error
      return null;
    }
  }

  public static RecordLocationWithSegmentId deserialize(byte[] recordLocationBytes, ByteBuffer reuse) {
    try {
      ByteBuffer byteBuffer = ByteBuffer.wrap(recordLocationBytes);
      reuse.rewind();
      reuse.put(recordLocationBytes);
      int segmentRef = reuse.getInt(0);
      if(segmentRef == -1) return null;

      int docId = reuse.getInt(Integer.BYTES);
      long comparisonValue = reuse.getLong(2 * Integer.BYTES);

      return new RecordLocationWithSegmentId(segmentRef, docId, comparisonValue);
    } catch (Exception e) {
      // log error
      return null;
    }
  }

  public static long getComparable(byte[] recordLocationBytes) {
    try {
      ByteBuffer byteBuffer = ByteBuffer.wrap(recordLocationBytes);
      return byteBuffer.getLong(2* Integer.BYTES);
    } catch (Exception e) {
      // log error
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    Schema schema = new Schema();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl("table", "segment", schema, System.currentTimeMillis());
    IndexSegment indexSegment = new EmptyIndexSegment(segmentMetadata);
    RecordLocationWithSegmentId recordLocation = new RecordLocationWithSegmentId(1, 1, System.currentTimeMillis());
    System.out.println(deserialize(serialize(recordLocation)));
  }
}
