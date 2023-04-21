package org.apache.hadoop.fs.s3a.scale;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.impl.ProgressListener;
import org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.S3_CRT_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_RECV_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_SEND_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getOutputStreamStatistics;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBytes;

@RunWith(Parameterized.class)
public class ITestS3ABenchmarkCRT extends S3AScaleTestBase {


  private static final Logger LOG = LoggerFactory.getLogger(
       ITestS3ABenchmarkCRT.class);
  public static final int DEFAULT_UPLOAD_BLOCKSIZE = 64 * _1KB;

  private Path scaleTestDir;

  private int uploadBlockSize = DEFAULT_UPLOAD_BLOCKSIZE;
  private int partitionSize;
  private long filesize;

  private long _1GB = 1024 * _1MB;

  private boolean crtEnabled;


  public ITestS3ABenchmarkCRT(String name, boolean crtEnabled) {
      this.crtEnabled = crtEnabled;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"crt_enable", true},
        {"crt_disabled", false},
    });
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    scaleTestDir = new Path("/crtbenchmark");
  }

  /**
   * Note that this can get called before test setup.
   * @return the configuration to use.
   */
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    partitionSize = (int) getTestPropertyBytes(conf,
        KEY_HUGE_PARTITION_SIZE,
        DEFAULT_HUGE_PARTITION_SIZE);
    assertTrue("Partition size too small: " + partitionSize,
        partitionSize >= MULTIPART_MIN_SIZE);
    conf.setLong(SOCKET_SEND_BUFFER, _1MB);
    conf.setLong(SOCKET_RECV_BUFFER, _1MB);
    conf.setLong(MIN_MULTIPART_THRESHOLD, partitionSize);
    conf.setInt(MULTIPART_SIZE, partitionSize);
    conf.set(USER_AGENT_PREFIX, "STestS3AHugeFileCreate");
    conf.set(FAST_UPLOAD_BUFFER, getBlockOutputBufferName());
    conf.setBoolean(S3_CRT_ENABLED, crtEnabled);
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }


//  @Test
//  public void benchmarkRename_256MB() throws IOException {
//    rename(256 * _1MB, "file_size_256M", 1);
////    rename(256 * _1MB, "file_size_256M", 2);
////    rename(256 * _1MB, "file_size_256M", 3);
//  }
//
//  @Test
//  public void benchmarkRename_1GB() throws IOException {
//    rename(1 * _1GB, "file_size_1GB", 1);
//  }
//
//  @Test
//  public void benchmarkRename_2GB() throws IOException {
//    rename(2 * _1GB, "file_size_2GB", 1);
//  }


    @Test
    public void benchmarkRename_5GB() throws IOException {
      rename(5 * _1GB, "file_size_5GB", 1);
    }

//  @Test
//  public void benchmarkRename_256MB() throws IOException {
//    createHugeFile(256 * _1MB, "file_size_256M_create_test_" + crtEnabled);
//  }

  private void rename(long size, String name, int iteration) throws IOException {

    LOG.info("Renaming {}, iteration {}", name, iteration);

    Path src = new Path(scaleTestDir + "/src", name);
    Path dest = new Path(scaleTestDir + "/dest", name);


    S3AFileSystem fs = getFileSystem();
    FileStatus fileStatus = probeFileStatus(src, fs);

    if(fileStatus == null) {
      createHugeFile(size, name);
    }

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.rename(src, dest);
    long mb = Math.max(size / _1MB, 1);
    LOG.info("Renamed {}, iteration {}", name, iteration);
    timer.end("time to rename file of %d MB", mb);
    LOG.info("Time per MB to rename = {} nS", toHuman(timer.nanosPerOperation(mb)));
    bandwidth(timer, size);


    FileStatus destinationFileStatus = fs.getFileStatus(dest);
    assertNotNull(destinationFileStatus);
    assertEquals(fs.makeQualified(dest), destinationFileStatus.getPath());

    //restore state
    fs.rename(dest, src);

    FileStatus srcFileStatus = fs.getFileStatus(src);
    assertNotNull(srcFileStatus);
    assertEquals(fs.makeQualified(src), srcFileStatus.getPath());

  }


  private FileStatus probeFileStatus(Path p,
      S3AFileSystem fs) throws IOException {
    try {
      return fs.getFileStatus(p);
    } catch (FileNotFoundException fnfe) {
      return null;
    }
  }


  @Override
  public void teardown() throws Exception {
    System.out.println("DO NOTHING OK");
  }

  /**
   * Get the name of this test suite, which is used in path generation.
   * Base implementation uses {@link #getBlockOutputBufferName()} for this.
   * @return the name of the suite.
   */
  public String getTestSuiteName() {
    return getBlockOutputBufferName();
  }



  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_ARRAY;
  }



  public void createHugeFile(long size, String name) throws IOException {

    filesize = size;
    long filesizeMB = filesize / _1MB;

    Path fileToCreate = new Path(scaleTestDir + "/src", name);
    describe("Creating file %s of size %d MB" +
            " with partition size %d buffered by %s",
        fileToCreate, filesizeMB, partitionSize, getBlockOutputBufferName());

    // now do a check of available upload time, with a pessimistic bandwidth
    // (that of remote upload tests). If the test times out then not only is
    // the test outcome lost, as the follow-on tests continue, they will
    // overlap with the ongoing upload test, for much confusion.
    int timeout = getTestTimeoutSeconds();
    // assume 1 MB/s upload bandwidth
    int bandwidth = _1MB;
    long uploadTime = filesize / bandwidth;
    assertTrue(String.format("Timeout set in %s seconds is too low;" +
                " estimating upload time of %d seconds at 1 MB/s." +
                " Rerun tests with -D%s=%d",
            timeout, uploadTime, KEY_TEST_TIMEOUT, uploadTime * 2),
        uploadTime < timeout);
    assertEquals("File size set in " + KEY_HUGE_FILESIZE + " = " + filesize
            + " is not a multiple of " + uploadBlockSize,
        0, filesize % uploadBlockSize);

    byte[] data = new byte[uploadBlockSize];
    for (int i = 0; i < uploadBlockSize; i++) {
      data[i] = (byte) (i % 256);
    }

    long blocks = filesize / uploadBlockSize;
    long blocksPerMB = _1MB / uploadBlockSize;

    // perform the upload.
    // there's lots of logging here, so that a tail -f on the output log
    // can give a view of what is happening.
    S3AFileSystem fs = getFileSystem();
    IOStatistics iostats = fs.getIOStatistics();

    String putRequests = Statistic.OBJECT_PUT_REQUESTS.getSymbol();
    String putBytes = Statistic.OBJECT_PUT_BYTES.getSymbol();
    Statistic putRequestsActive = Statistic.OBJECT_PUT_REQUESTS_ACTIVE;
    Statistic putBytesPending = Statistic.OBJECT_PUT_BYTES_PENDING;

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    BlockOutputStreamStatistics streamStatistics;
    long blocksPer10MB = blocksPerMB * 10;
    ProgressCallback progress = new ProgressCallback(timer);
    try (FSDataOutputStream out = fs.create(fileToCreate,
        true,
        uploadBlockSize,
        progress)) {
      try {
        streamStatistics = getOutputStreamStatistics(out);
      } catch (ClassCastException e) {
        LOG.info("Wrapped output stream is not block stream: {}",
            out.getWrappedStream());
        streamStatistics = null;
      }

      for (long block = 1; block <= blocks; block++) {
        out.write(data);
        long written = block * uploadBlockSize;
        // every 10 MB and on file upload @ 100%, print some stats
        if (block % blocksPer10MB == 0 || written == filesize) {
          long percentage = written * 100 / filesize;
          double elapsedTime = timer.elapsedTime() / 1.0e9;
          double writtenMB = 1.0 * written / _1MB;
          LOG.info(String.format("[%02d%%] Buffered %.2f MB out of %d MB;" +
                  " PUT %d bytes (%d pending) in %d operations (%d active);" +
                  " elapsedTime=%.2fs; write to buffer bandwidth=%.2f MB/s",
              percentage,
              writtenMB,
              filesizeMB,
              iostats.counters().get(putBytes),
              gaugeValue(putBytesPending),
              iostats.counters().get(putRequests),
              gaugeValue(putRequestsActive),
              elapsedTime,
              writtenMB / elapsedTime));
        }
      }
      // now close the file
      LOG.info("Closing stream {}", out);
      LOG.info("Statistics : {}", streamStatistics);
      ContractTestUtils.NanoTimer closeTimer
          = new ContractTestUtils.NanoTimer();
      out.close();
      closeTimer.end("time to close() output stream");
    }

    timer.end("time to write %d MB in blocks of %d",
        filesizeMB, uploadBlockSize);
    bandwidth(timer, filesize);
  }

  /**
   * Progress callback.
   */
  private final class ProgressCallback implements Progressable, ProgressListener {
    private AtomicLong bytesTransferred = new AtomicLong(0);
    private AtomicInteger failures = new AtomicInteger(0);
    private final ContractTestUtils.NanoTimer timer;

    private ProgressCallback(ContractTestUtils.NanoTimer timer) {
      this.timer = timer;
    }

    @Override
    public void progress() {
    }

    @Override
    public void progressChanged(ProgressListenerEvent eventType, int transferredBytes) {

      switch (eventType) {
      case TRANSFER_PART_FAILED_EVENT:
        // failure
        failures.incrementAndGet();
        LOG.warn("Transfer failure");
        break;
      case TRANSFER_PART_COMPLETED_EVENT:
        // completion
        bytesTransferred.addAndGet(transferredBytes);
        long elapsedTime = timer.elapsedTime();
        double elapsedTimeS = elapsedTime / 1.0e9;
        long written = bytesTransferred.get();
        long writtenMB = written / _1MB;
        LOG.info(String.format(
            "Event %s; total uploaded=%d MB in %.1fs;" +
                " effective upload bandwidth = %.2f MB/s",
            eventType,
            writtenMB, elapsedTimeS, writtenMB / elapsedTimeS));
        break;
      default:
        // nothing
        break;
      }
    }

    public String toString() {
      String sb = "ProgressCallback{"
          + "bytesTransferred=" + bytesTransferred +
          ", failures=" + failures +
          '}';
      return sb;
    }

    private void verifyNoFailures(String operation) {
      assertEquals("Failures in " + operation + ": " + this, 0, failures.get());
    }
  }


  /**
   * Delete any file, time how long it took.
   * @param path path to delete
   * @param recursive recursive flag
   */
  protected void delete(Path path, boolean recursive) throws IOException {
    describe("Deleting %s", path);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    getFileSystem().delete(path, recursive);
    timer.end("time to delete %s", path);
  }


}
