package org.apache.pinot.segment.spi.memory;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link PinotOutputStream} that writes to a sequence of pages.
 * <p>
 * When a page is full, a new page is allocated and writing continues there.
 * The page size is determined by the {@link PageAllocator} used to create the stream.
 * Some {@link PageAllocator} implementations may support releasing pages, which can be useful to reduce memory usage.
 * The recommended page size goes from 4KBs to 1MB, depending on the use case, with a sweet spot.
 * {@link PageAllocator#MIN_RECOMMENDED_PAGE_SIZE} and {@link PageAllocator#MAX_RECOMMENDED_PAGE_SIZE} can be used to
 * determine the recommended page size for the current architecture.
 * <p>
 * This class is specially useful when writing data whose final size is not known in advance or when it is needed to
 * {@link #seek(long) seek} to a specific position in the stream.
 * Writes that cross page boundaries are handled transparently, although a performance penalty will be paid.
 * <p>
 * Data written in this stream can be retrieved as a list of {@link ByteBuffer} pages using {@link #getPages()}, which
 * can be directly read using {@link CompoundDataBuffer} or send to the network as using
 * {@link com.google.protobuf.UnsafeByteOperations#unsafeWrap(byte[]) GRPC}.
 */
public class PagedPinotOutputStream extends PinotOutputStream {
  private final PageAllocator _allocator;
  private final int _pageSize;
  private final ArrayList<ByteBuffer> _pages;
  private ByteBuffer _currentPage;
  private long _currentPageStartOffset;
  private int _offsetInPage;

  private static final Logger LOGGER = LoggerFactory.getLogger(PagedPinotOutputStream.class);

  public PagedPinotOutputStream(PageAllocator allocator) {
    _pageSize = allocator.pageSize();
    _allocator = allocator;
    _pages = new ArrayList<>(8);
    _currentPage = _allocator.allocate().order(ByteOrder.BIG_ENDIAN);
    _currentPageStartOffset = 0;
    _pages.add(_currentPage);
  }

  private void nextPage() {
    moveCurrentOffset(remainingInPage());
  }

  private int remainingInPage() {
    return _pageSize - _offsetInPage;
  }

  /**
   * Returns a read only view of the pages written so far.
   * <p>
   * All pages but the last one will be {@link ByteBuffer#clear() cleared}.
   * The latest page will have its position set to 0 and its limit set to the last byte written.
   */
  public ByteBuffer[] getPages() {
    int numPages = _pages.size();
    if (numPages == 0) {
      return new ByteBuffer[0];
    }
    ByteBuffer[] result = new ByteBuffer[numPages];
    for (int i = 0; i < _pages.size(); i++) {
      ByteBuffer byteBuffer = _pages.get(i);
      ByteBuffer page = byteBuffer.asReadOnlyBuffer();
      page.clear();
      result[i] = page;
    }
    ByteBuffer lastPage = result[numPages - 1];
    lastPage.limit(_offsetInPage);
    return result;
  }

  @Override
  public long getCurrentOffset() {
    return _currentPageStartOffset + _offsetInPage;
  }

  @Override
  public void seek(long newPos) {
    if (newPos < 0) {
      throw new IllegalArgumentException("New position cannot be negative");
    }
    if (newPos == 0) {
      _currentPage = _pages.get(0);
      _offsetInPage = 0;
    } else {
      int pageIdx = (int) (newPos / _pageSize);
      if (pageIdx >= _pages.size()) {
        _pages.ensureCapacity(pageIdx + 1);
        while (_pages.size() <= pageIdx) {
          _pages.add(_allocator.allocate().order(ByteOrder.BIG_ENDIAN));
        }
      }
      int offsetInPage = (int) (newPos % _pageSize);
      _currentPage = _pages.get(pageIdx);
      _currentPageStartOffset = pageIdx * (long) _pageSize;
      _offsetInPage = offsetInPage;
    }
  }

  @Override
  public void write(int b)
      throws IOException {
    if (remainingInPage() == 0) {
      nextPage();
    }
    _currentPage.put(_offsetInPage++, (byte) b);
  }

  @Override
  public void writeInt(int v)
      throws IOException {
    if (remainingInPage() >= Integer.BYTES) {
      _currentPage.putInt(_offsetInPage, v);
      _offsetInPage += Integer.BYTES;
    } else {
      super.writeInt(v);
    }
  }

  @Override
  public void writeLong(long v)
      throws IOException {
    if (remainingInPage() >= Long.BYTES) {
      _currentPage.putLong(_offsetInPage, v);
      _offsetInPage += Long.BYTES;
    } else {
      super.writeLong(v);
    }
  }

  @Override
  public void writeShort(int v)
      throws IOException {
    if (remainingInPage() >= Short.BYTES) {
      _currentPage.putShort(_offsetInPage, (short) v);
      _offsetInPage += Short.BYTES;
    } else {
      super.writeShort(v);
    }
  }

  @Override
  public void write(byte[] b, int off, int len)
      throws IOException {
    if (remainingInPage() >= len) {
      _currentPage.put(b, off, len);
      _offsetInPage += len;
    } else {
      int written = 0;
      while (written < len) {
        int remainingInPage = remainingInPage();
        if (remainingInPage == 0) {
          nextPage();
          continue;
        }
        int toWrite = Math.min(len - written, remainingInPage);
        _currentPage.put(b, off + written, toWrite);
        written += toWrite;
        _offsetInPage += toWrite;
      }
    }
  }

  @Override
  public void write(DataBuffer input, long offset, long length)
      throws IOException {
    if (remainingInPage() >= length) {
      int intLength = (int) length;
      input.copyTo(offset, _currentPage, _offsetInPage, intLength);
      _offsetInPage += intLength;
    } else {
      long written = 0;
      while (written < length) {
        int remainingInPage = remainingInPage();
        if (remainingInPage == 0) {
          nextPage();
          continue;
        }
        int toWrite = (int) Math.min(length - written, remainingInPage);
        input.copyTo(offset + written, _currentPage, _offsetInPage, toWrite);
        written += toWrite;
        _offsetInPage += toWrite;
      }
    }
  }

  public CompoundDataBuffer asBuffer(ByteOrder order, boolean owner) {
    List<DataBuffer> pages = Arrays.stream(getPages())
        .map(PinotByteBuffer::wrap)
        .collect(Collectors.toList());
    return new CompoundDataBuffer(pages, order, owner);
  }

  @Override
  public void close()
      throws IOException {
    IOException ex = null;
    for (ByteBuffer page : _pages) {
      try {
        _allocator.release(page);
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  public static abstract class PageAllocator {
    public static final int MIN_RECOMMENDED_PAGE_SIZE;
    public static final int MAX_RECOMMENDED_PAGE_SIZE;

    static {
      int minRecommendedPageSize = -1;
      int maxRecommendedPageSize = -1;
      try {
        switch (ArchUtils.getProcessor().getType()) {
          case AARCH_64:
            // ARM processors support 4KB and 1MB pages
            minRecommendedPageSize = 16 * 1024;
            maxRecommendedPageSize = 1024 * 1024;
            break;
          case X86:
          default:
            // X86 processors support 4KB and 4MB pages
            minRecommendedPageSize = 4 * 1024;
            maxRecommendedPageSize = 4 * 1024 * 1024;
            break;
        }
      } catch (Throwable t) {
        LOGGER.warn("Could not determine processor architecture. Falling back to default values", t);
        // Fallback to 4KB and 4MBs
        minRecommendedPageSize = 4 * 1024;
        maxRecommendedPageSize = 4 * 1024 * 1024;
      }
      MIN_RECOMMENDED_PAGE_SIZE = minRecommendedPageSize;
      MAX_RECOMMENDED_PAGE_SIZE = maxRecommendedPageSize;
    }

    public abstract int pageSize();

    public abstract ByteBuffer allocate();

    public abstract void release(ByteBuffer buffer)
        throws IOException;
  }

  public static class HeapPageAllocator extends PageAllocator {

    private final int _pageSize;

    public static HeapPageAllocator createSmall() {
      return new HeapPageAllocator(MIN_RECOMMENDED_PAGE_SIZE);
    }

    public static HeapPageAllocator createLarge() {
      return new HeapPageAllocator(MAX_RECOMMENDED_PAGE_SIZE);
    }

    public HeapPageAllocator(int pageSize) {
      Preconditions.checkArgument(pageSize > 0, "Page size must be positive");
      _pageSize = pageSize;
    }

    @Override
    public int pageSize() {
      return _pageSize;
    }

    @Override
    public ByteBuffer allocate() {
      return ByteBuffer.allocate(_pageSize);
    }

    @Override
    public void release(ByteBuffer buffer) {
      // Do nothing
    }
  }

  public static class DirectPageAllocator extends PageAllocator {

    private final int _pageSize;
    private final boolean _release;

    public DirectPageAllocator(int pageSize) {
      this(pageSize, false);
    }

    public DirectPageAllocator(int pageSize, boolean release) {
      Preconditions.checkArgument(pageSize > 0, "Page size must be positive");
      _pageSize = pageSize;
      _release = release;
    }

    public static DirectPageAllocator createSmall(boolean release) {
      return new DirectPageAllocator(MIN_RECOMMENDED_PAGE_SIZE, release);
    }

    public static DirectPageAllocator createLarge(boolean release) {
      return new DirectPageAllocator(MAX_RECOMMENDED_PAGE_SIZE, release);
    }

    @Override
    public int pageSize() {
      return _pageSize;
    }

    @Override
    public ByteBuffer allocate() {
      return ByteBuffer.allocateDirect(_pageSize);
    }

    @Override
    public void release(ByteBuffer buffer)
        throws IOException {
      if (_release && CleanerUtil.UNMAP_SUPPORTED) {
        CleanerUtil.getCleaner().freeBuffer(buffer);
      }
    }
  }
}
