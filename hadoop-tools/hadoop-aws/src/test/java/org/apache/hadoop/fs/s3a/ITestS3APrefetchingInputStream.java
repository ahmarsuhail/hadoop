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

package org.apache.hadoop.fs.s3a;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.prefetch.S3APrefetchingInputStream;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMaximum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticGaugeValue;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ACTIVE_MEMORY_IN_USE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCKS_IN_FILE_CACHE;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Test the prefetching input stream, validates that the underlying S3ACachingInputStream and
 * S3AInMemoryInputStream are working as expected.
 */
public class ITestS3APrefetchingInputStream extends AbstractS3ACostTest {

  public ITestS3APrefetchingInputStream() {
    super(true);
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingInputStream.class);

  private static final int S_1K = 1024;
  private static final int S_1M = S_1K * S_1K;
  // Path for file which should have length > block size so S3ACachingInputStream is used
  private Path largeFile;
  private FileSystem largeFileFS;
  private int numBlocks;
  private int blockSize;
  private long largeFileSize;
  // Size should be < block size so S3AInMemoryInputStream is used
  private static final int SMALL_FILE_SIZE = S_1K * 16;


  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    return conf;
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    cleanupWithLogger(LOG, largeFileFS);
    largeFileFS = null;
  }

  private void openFS() throws Exception {
    Configuration conf = getConfiguration();
    String largeFileUri = S3ATestUtils.getCSVTestFile(conf);

    largeFile = new Path(largeFileUri);
    blockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    largeFileFS = new S3AFileSystem();
    largeFileFS.initialize(new URI(largeFileUri), getConfiguration());
    FileStatus fileStatus = largeFileFS.getFileStatus(largeFile);
    largeFileSize = fileStatus.getLen();
    numBlocks = calculateNumBlocks(largeFileSize, blockSize);
  }

  private static int calculateNumBlocks(long largeFileSize, int blockSize) {
    if (largeFileSize == 0) {
      return 0;
    } else {
      return ((int) (largeFileSize / blockSize)) + (largeFileSize % blockSize > 0 ? 1 : 0);
    }
  }

  @Test
  public void testReadLargeFileFully() throws Throwable {
    describe("read a large file fully, uses S3ACachingInputStream");
    IOStatistics ioStats;
    openFS();

    try (FSDataInputStream in = largeFileFS.open(largeFile)) {
      ioStats = in.getIOStatistics();

      byte[] buffer = new byte[S_1M * 10];
      long bytesRead = 0;

      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
      while (bytesRead < largeFileSize) {
        in.readFully(buffer, 0, (int) Math.min(buffer.length, largeFileSize - bytesRead));
        bytesRead += buffer.length;
        // Blocks are fully read, no blocks should be cached
        verifyStatisticGaugeValue(ioStats, STREAM_READ_BLOCKS_IN_FILE_CACHE,
            0);
      }
      timer.end("Read file fully with Prefetching and CRT");

      // Assert that first block is read synchronously, following blocks are prefetched
      verifyStatisticCounterValue(ioStats, STREAM_READ_PREFETCH_OPERATIONS,
          numBlocks - 1);
      verifyStatisticCounterValue(ioStats, ACTION_HTTP_GET_REQUEST, numBlocks);
      verifyStatisticCounterValue(ioStats, STREAM_READ_OPENED, numBlocks);
    }
    // Verify that once stream is closed, all memory is freed
    verifyStatisticGaugeValue(ioStats, STREAM_READ_ACTIVE_MEMORY_IN_USE, 0);
    assertThatStatisticMaximum(ioStats,
        ACTION_EXECUTOR_ACQUIRED + SUFFIX_MAX).isGreaterThan(0);
  }


  @Test
  public void testReadLargeFileFullyWithCRT() throws Throwable {
    describe("read a large file fully, uses S3ACachingInputStream");
    S3AsyncClient s3AsyncClient = getFileSystem().getS3AsyncClient();

    GetObjectRequest request = GetObjectRequest.builder()
        .bucket("ahmarsu-test-aws-s3a")
        .key("crtbenchmark/src/file_size_256M")
        .build();

    CompletableFuture<ResponseInputStream<GetObjectResponse>> responseFuture =
        s3AsyncClient.getObject(request, AsyncResponseTransformer.toBlockingInputStream());

    int bytesRead = 0;

    try (ResponseInputStream<GetObjectResponse> responseStream = responseFuture.join()) {
      GetObjectResponse r = responseStream.response();
      byte[] buffer = new byte[S_1M * 10];
      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
      while(bytesRead < r.contentLength()) {
        int bytes = responseStream.read(buffer);
        bytesRead += bytes;
        System.out.println("Bytes read");
        System.out.println(bytesRead);
      }
      timer.end("Test read file fully with CRT client, without prefetching");
    }

  }

  @Test
  public void testReadLargeFileFullyWithoutPrefetchingAndCRT() throws Throwable {
    describe("read a large file fully, uses S3ACachingInputStream");
    S3Client s3Client = getFileSystem().getS3Client();

    GetObjectRequest request = GetObjectRequest.builder()
        .bucket("ahmarsu-test-aws-s3a")
        .key("crtbenchmark/src/file_size_256M")
        .build();


    int bytesRead = 0;

    try (ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(request)) {
      GetObjectResponse r = responseStream.response();
      byte[] buffer = new byte[S_1M * 10];
      ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
      while(bytesRead < r.contentLength()) {
        int bytes = responseStream.read(buffer);
        bytesRead += bytes;
        System.out.println("Bytes read");
        System.out.println(bytesRead);
      }
      timer.end("Test read file fully with sync client, without prefetching");
    }

  }

}
