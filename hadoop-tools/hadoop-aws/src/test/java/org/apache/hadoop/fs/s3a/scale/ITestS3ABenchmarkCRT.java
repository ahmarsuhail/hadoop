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
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_RECV_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_SEND_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getOutputStreamStatistics;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBytes;

public class ITestS3ABenchmarkCRT extends S3AScaleTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3ABenchmarkCRT.class);
  public static final int DEFAULT_UPLOAD_BLOCKSIZE = 64 * _1KB;

  private Path scaleTestDir;

  private int uploadBlockSize = DEFAULT_UPLOAD_BLOCKSIZE;
  private int partitionSize;
  private long filesize;

  private long _1GB = 1024 * _1MB;

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
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }


  @Test
  public void benchmarkRename_256MB() throws IOException {
    rename(256 * _1MB, "file_size_256M", 1);
    //    rename(256 * _1MB, "file_size_256M", 2);
    //    rename(256 * _1MB, "file_size_256M", 3);
  }

  @Test
  public void benchmarkRename_1GB() throws IOException {
    rename(1 * _1GB, "file_size_1GB", 1);
  }

  @Test
  public void benchmarkRename_2GB() throws IOException {
    rename(2 * _1GB, "file_size_2GB", 1);
  }

  private void rename(long size, String name, int iteration) throws IOException {

    LOG.info("Renaming {}, iteration {}", name, iteration);

    Path src = new Path(scaleTestDir + "/src", name);
    Path dest = new Path(scaleTestDir + "/dest", name);


    S3AFileSystem fs = getFileSystem();
    FileStatus fileStatus = probeFileStatus(src, fs);


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
