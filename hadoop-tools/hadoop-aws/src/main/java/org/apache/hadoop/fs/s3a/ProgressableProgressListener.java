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

import org.slf4j.Logger;

import software.amazon.awssdk.transfer.s3.progress.TransferListener;

import org.apache.hadoop.util.Progressable;

/**
 * Listener to progress from AWS regarding transfers.
 */
public class ProgressableProgressListener implements TransferListener {
  private static final Logger LOG = S3AFileSystem.LOG;
  private final S3AFileSystem fs;
  private final String key;
  private final Progressable progress;
  private long lastBytesTransferred;

  /**
   * Instantiate.
   * @param fs filesystem: will be invoked with statistics updates
   * @param key key for the upload
   * @param progress optional callback for progress.
   */
  public ProgressableProgressListener(S3AFileSystem fs,
      String key,
      Progressable progress) {
    this.fs = fs;
    this.key = key;
    this.progress = progress;
    this.lastBytesTransferred = 0;
  }

  @Override
  public void  transferInitiated(TransferListener.Context.TransferInitiated context) {
    fs.incrementWriteOperations();
  }

  @Override
  public void transferComplete(TransferListener.Context.TransferComplete context) {
    fs.incrementWriteOperations();
  }

  @Override
  public void bytesTransferred(TransferListener.Context.BytesTransferred context) {

    if(progress != null) {
      progress.progress();
    }

    long transferred = context.progressSnapshot().bytesTransferred();
    long delta = transferred - lastBytesTransferred;
    fs.incrementPutProgressStatistics(key, delta);
    lastBytesTransferred = transferred;
  }

  /**
   * Method to get the last recorded number of bytes transferred for an object.
   * @return number of bytes transferred 
   */
  public long getLastBytesTransferred() {
    return lastBytesTransferred;
  }

}
