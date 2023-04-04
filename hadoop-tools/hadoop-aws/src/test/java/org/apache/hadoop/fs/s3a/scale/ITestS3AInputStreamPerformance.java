/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_BUFFER_SIZE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getInputStreamStatistics;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getS3AInputStream;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMinimum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMaximumStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MIN;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Look at the performance of S3a Input Stream Reads.
 */
public class ITestS3AInputStreamPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AInputStreamPerformance.class);
  private static final int READAHEAD_128K = 128 * _1KB;

  private S3AFileSystem s3aFS;
  private Path testData;
  private FileStatus testDataStatus;
  private FSDataInputStream in;
  private S3AInputStreamStatistics streamStatistics;
  public static final int BLOCK_SIZE = 32 * 1024;
  public static final int BIG_BLOCK_SIZE = 256 * 1024;

  private static final IOStatisticsSnapshot IOSTATS = snapshotIOStatistics();


  /** Tests only run if the there is a named test file that can be read. */
  private boolean testDataAvailable = true;
  private String assumptionMessage = "test file";

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, false);
    return conf;
  }

  /**
   * Open the FS and the test data. The input stream is always set up here.
   * @throws IOException IO Problems.
   */
  @Before
  public void openFS() throws IOException {
    Configuration conf = getConf();
    conf.setInt(SOCKET_SEND_BUFFER, 16 * 1024);
    conf.setInt(SOCKET_RECV_BUFFER, 16 * 1024);
    String testFile =  conf.getTrimmed(KEY_CSVTEST_FILE,
        "s3a://ahmarsu-test-aws-s3a/crtbenchmark/src/file_size_256M");
    if (testFile.isEmpty()) {
      assumptionMessage = "Empty test property: " + KEY_CSVTEST_FILE;
      LOG.warn(assumptionMessage);
      testDataAvailable = false;
    } else {
      testData = new Path(testFile);
      LOG.info("Using {} as input stream source", testData);
      Path path = this.testData;
      bindS3aFS(path);
      try {
        testDataStatus = s3aFS.getFileStatus(this.testData);
      } catch (IOException e) {
        LOG.warn("Failed to read file {} specified in {}",
            testFile, KEY_CSVTEST_FILE, e);
        throw e;
      }
    }
  }

  private void bindS3aFS(Path path) throws IOException {
    s3aFS = (S3AFileSystem) FileSystem.newInstance(path.toUri(), getConf());
  }

  /**
   * Cleanup: close the stream, close the FS.
   */
  @After
  public void cleanup() {
    describe("cleanup");
    IOUtils.closeStream(in);
    if (in != null) {
      final IOStatistics stats = in.getIOStatistics();
      LOG.info("Stream statistics {}",
          ioStatisticsToPrettyString(stats));
      IOSTATS.aggregate(stats);
    }
    if (s3aFS != null) {
      final IOStatistics stats = s3aFS.getIOStatistics();
      LOG.info("FileSystem statistics {}",
          ioStatisticsToPrettyString(stats));
      FILESYSTEM_IOSTATS.aggregate(stats);
      IOUtils.closeStream(s3aFS);
    }
  }

  @AfterClass
  public static void dumpIOStatistics() {
    LOG.info("Aggregate Stream Statistics {}", IOSTATS);
  }

  /**
   * Declare that the test requires the CSV test dataset.
   */
  private void requireCSVTestData() {
    assume(assumptionMessage, testDataAvailable);
  }

  /**
   * Open the test file with the read buffer specified in the setting.
   * {@link #KEY_READ_BUFFER_SIZE}; use the {@code Normal} policy
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile() throws IOException {
    return openTestFile(S3AInputPolicy.Normal, 0);
  }

  /**
   * Open the test file with the read buffer specified in the setting
   * {@link #KEY_READ_BUFFER_SIZE}.
   * This includes the {@link #requireCSVTestData()} assumption; so
   * if called before any FS op, will automatically skip the test
   * if the CSV file is absent.
   *
   * @param inputPolicy input policy to use
   * @param readahead readahead/buffer size
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile(S3AInputPolicy inputPolicy, long readahead)
      throws IOException {
    requireCSVTestData();
    return openDataFile(s3aFS, testData, inputPolicy, readahead, testDataStatus.getLen());
  }

  /**
   * Open a test file with the read buffer specified in the setting
   * {@link org.apache.hadoop.fs.s3a.S3ATestConstants#KEY_READ_BUFFER_SIZE}.
   *
   * @param path path to open
   * @param inputPolicy input policy to use
   * @param readahead readahead/buffer size
   * @param length
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  private FSDataInputStream openDataFile(S3AFileSystem fs,
      Path path,
      S3AInputPolicy inputPolicy,
      long readahead,
      final long length) throws IOException {
    int bufferSize = getConf().getInt(KEY_READ_BUFFER_SIZE,
        DEFAULT_READ_BUFFER_SIZE);
    final FutureDataInputStreamBuilder builder = fs.openFile(path)
        .opt(FS_OPTION_OPENFILE_READ_POLICY,
            inputPolicy.toString())
        .opt(FS_OPTION_OPENFILE_LENGTH, length)
        .opt(FS_OPTION_OPENFILE_BUFFER_SIZE, bufferSize);
    if (readahead > 0) {
      builder.opt(READAHEAD_RANGE, readahead);
    }
    FSDataInputStream stream = awaitFuture(builder.build());
    streamStatistics = getInputStreamStatistics(stream);
    return stream;
  }

  /**
   * Assert that the stream was only ever opened once.
   */
  protected void assertStreamOpenedExactlyOnce() {
    assertOpenOperationCount(1);
  }

  /**
   * Make an assertion count about the number of open operations.
   * @param expected the expected number
   */
  private void assertOpenOperationCount(long expected) {
    assertEquals("open operations in\n" + in,
        expected, streamStatistics.getOpenOperations());
  }

  /**
   * Log how long an IOP took, by dividing the total time by the
   * count of operations, printing in a human-readable form.
   * @param operation operation being measured
   * @param timer timing data
   * @param count IOP count.
   */
  protected void logTimePerIOP(String operation,
      NanoTimer timer,
      long count) {
    LOG.info("Time per {}: {} nS",
        operation, toHuman(timer.duration() / count));
  }

  @Test
  public void testTimeToOpenAndReadWholeFileBlocks() throws Throwable {
    skipIfClientSideEncryption();
    requireCSVTestData();
    int blockSize = _1MB * 10;
    describe("Open the test file %s and read it in blocks of size %d",
        testData, blockSize);
    long len = testDataStatus.getLen();
    in = openTestFile();
    byte[] block = new byte[blockSize];
    NanoTimer timer2 = new NanoTimer();
    long count = 0;
    // implicitly rounding down here
    long blockCount = len / blockSize;
    long totalToRead = blockCount * blockSize;
    long minimumBandwidth = 128 * 1024;
    int maxResetCount = 4;
    int resetCount = 0;
    for (long i = 0; i < blockCount; i++) {
      int offset = 0;
      int remaining = blockSize;
      long blockId = i + 1;
      NanoTimer blockTimer = new NanoTimer();
      int reads = 0;
      while (remaining > 0) {
        NanoTimer readTimer = new NanoTimer();
        int bytesRead = in.read(block, offset, remaining);
        reads++;
        if (bytesRead == 1) {
          break;
        }
        remaining -= bytesRead;
        offset += bytesRead;
        count += bytesRead;
        readTimer.end();
        if (bytesRead != 0) {
          LOG.debug("Bytes in read #{}: {} , block bytes: {}," +
                  " remaining in block: {}" +
                  " duration={} nS; ns/byte: {}, bandwidth={} MB/s",
              reads, bytesRead, blockSize - remaining, remaining,
              readTimer.duration(),
              readTimer.nanosPerOperation(bytesRead),
              readTimer.bandwidthDescription(bytesRead));
        } else {
          LOG.warn("0 bytes returned by read() operation #{}", reads);
        }
      }
      blockTimer.end("Reading block %d in %d reads", blockId, reads);
      String bw = blockTimer.bandwidthDescription(blockSize);
      LOG.info("Bandwidth of block {}: {} MB/s: ", blockId, bw);
      if (bandwidth(blockTimer, blockSize) < minimumBandwidth) {
        LOG.warn("Bandwidth {} too low on block {}: resetting connection",
            bw, blockId);
        Assertions.assertThat(resetCount)
            .describedAs("Bandwidth of %s too low after  %s attempts",
                bw, resetCount)
            .isLessThanOrEqualTo(maxResetCount);
        resetCount++;
        // reset the connection
        getS3AInputStream(in).resetConnection();
      }
    }
    timer2.end("Time to read %d bytes in %d blocks", totalToRead, blockCount);
    LOG.info("Overall Bandwidth {} MB/s; reset connections {}",
        timer2.bandwidth(totalToRead), resetCount);
  }

  /**
   * Work out the bandwidth in bytes/second.
   * @param timer timer measuring the duration
   * @param bytes bytes
   * @return the number of bytes/second of the recorded operation
   */
  public static double bandwidth(NanoTimer timer, long bytes) {
    return bytes * 1.0e9 / timer.duration();
  }


}
