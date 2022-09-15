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

package org.apache.hadoop.fs.s3a.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.utils.Md5Utils;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecretOperations;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DEFAULT_UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkNotNull;

/**
 * The standard implementation of the request factory.
 * This creates AWS SDK request classes for the specific bucket,
 * with standard options/headers set.
 * It is also where custom setting parameters can take place.
 *
 * All creation of AWS S3 requests MUST be through this class so that
 * common options (encryption etc.) can be added here,
 * and so that any chained transformation of requests can be applied.
 *
 * This is where audit span information is added to the requests,
 * until it is done in the AWS SDK itself.
 *
 * All created requests will be passed through
 * {@link PrepareRequest#prepareRequest(AmazonWebServiceRequest)} before
 * being returned to the caller.
 */
public class RequestFactoryImpl implements RequestFactory {

  public static final Logger LOG = LoggerFactory.getLogger(
      RequestFactoryImpl.class);

  /**
   * Target bucket.
   */
  private final String bucket;

  /**
   * Encryption secrets.
   */
  private EncryptionSecrets encryptionSecrets;

  /**
   * ACL For new objects.
   */
  // TODO: Set this to V2's ObjectCannedACL, and replace cannedACL.toString().
  //  To be done during MPU update work.
  private final CannedAccessControlList cannedACL;

  /**
   * Max number of multipart entries allowed in a large
   * upload. Tunable for testing only.
   */
  private final long multipartPartCountLimit;

  /**
   * Callback to prepare requests.
   */
  private final PrepareRequest requestPreparer;

  /**
   * Content encoding (null for none).
   */
  private final String contentEncoding;

  /**
   * Storage class.
   */
  private final StorageClass storageClass;

  /**
   * Constructor.
   * @param builder builder with all the configuration.
   */
  protected RequestFactoryImpl(
      final RequestFactoryBuilder builder) {
    this.bucket = builder.bucket;
    this.cannedACL = builder.cannedACL;
    this.encryptionSecrets = builder.encryptionSecrets;
    this.multipartPartCountLimit = builder.multipartPartCountLimit;
    this.requestPreparer = builder.requestPreparer;
    this.contentEncoding = builder.contentEncoding;
    this.storageClass = builder.storageClass;
  }

  /**
   * Preflight preparation of AWS request.
   * @param <T> web service request
   * @return prepared entry.
   */
  @Retries.OnceRaw
  private <T extends AmazonWebServiceRequest> T prepareRequest(T t) {
    return requestPreparer != null
        ? requestPreparer.prepareRequest(t)
        : t;
  }

  /**
   * Get the canned ACL of this FS.
   * @return an ACL, if any
   */
  @Override
  public CannedAccessControlList getCannedACL() {
    return cannedACL;
  }

  /**
   * Get the target bucket.
   * @return the bucket.
   */
  protected String getBucket() {
    return bucket;
  }

  /**
   * Create the AWS SDK structure used to configure SSE,
   * if the encryption secrets contain the information/settings for this.
   * @return an optional set of KMS Key settings
   */
  @Override
  public Optional<SSEAwsKeyManagementParams> generateSSEAwsKeyParams() {
    return EncryptionSecretOperations.createSSEAwsKeyManagementParams(
        encryptionSecrets);
  }

  /**
   * Create the SSE-C structure for the AWS SDK, if the encryption secrets
   * contain the information/settings for this.
   * This will contain a secret extracted from the bucket/configuration.
   * @return an optional customer key.
   */
  // TODO: This doesn't need to be optional anymore, update during getObject work.
  @Override
  public Optional<SSECustomerKey> generateSSECustomerKey() {
    return EncryptionSecretOperations.createSSECustomerKey(
        encryptionSecrets);
  }

  /**
   * Get the encryption algorithm of this endpoint.
   * @return the encryption algorithm.
   */
  @Override
  public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
    return encryptionSecrets.getEncryptionMethod();
  }

  /**
   * Get the content encoding (e.g. gzip) or return null if none.
   * @return content encoding
   */
  @Override
  public String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * Get the object storage class, return null if none.
   * @return storage class
   */
  @Override
  public StorageClass getStorageClass() {
    return storageClass;
  }

  /**
   * Sets server side encryption parameters to the part upload
   * request when encryption is enabled.
   * @param request upload part request
   */
  protected void setOptionalUploadPartRequestParameters(
      UploadPartRequest request) {
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  /**
   * Set the optional parameters when initiating the request (encryption,
   * headers, storage, etc).
   * @param request request to patch.
   */
  protected void setOptionalMultipartUploadRequestParameters(
      InitiateMultipartUploadRequest request) {
    generateSSEAwsKeyParams().ifPresent(request::setSSEAwsKeyManagementParams);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
  }

  private CopyObjectRequest.Builder buildCopyObjectRequest() {

    CopyObjectRequest.Builder copyObjectRequestBuilder = CopyObjectRequest.builder();

    final S3AEncryptionMethods algorithm
        = getServerSideEncryptionAlgorithm();

    if (contentEncoding != null) {
      copyObjectRequestBuilder.contentEncoding(contentEncoding);
    }

    return copyObjectRequestBuilder;
  }

  @Override
  public CopyObjectRequest newCopyObjectRequest(String srcKey,
      String dstKey,
      HeadObjectResponse srcom) {

    CopyObjectRequest.Builder copyObjectRequestBuilder = buildCopyObjectRequest();

    Map<String, String> dstom = new HashMap<>();
    HeaderProcessing.cloneObjectMetadata(srcom, dstom, copyObjectRequestBuilder);
    copyEncryptionParameters(srcom, copyObjectRequestBuilder);
    if (cannedACL != null) {
      copyObjectRequestBuilder.acl(cannedACL.toString());
    }
    copyObjectRequestBuilder.metadata(dstom);
    if (srcom.storageClass() != null) {
      copyObjectRequestBuilder.storageClass(srcom.storageClass());
    }

    return copyObjectRequestBuilder.destinationBucket(getBucket())
        .destinationKey(dstKey).sourceBucket(getBucket()).sourceKey(srcKey).build();
  }

  /**
   * Propagate encryption parameters from source file if set else use the
   * current filesystem encryption settings.
   * @param srcom source object metadata.
   * @param copyObjectRequestBuilder copy object request builder.
   */
  protected void copyEncryptionParameters(
      HeadObjectResponse srcom,
      CopyObjectRequest.Builder copyObjectRequestBuilder) {

    final S3AEncryptionMethods algorithm
        = getServerSideEncryptionAlgorithm();

    if (S3AEncryptionMethods.SSE_S3 == algorithm) {
      copyObjectRequestBuilder.serverSideEncryption(algorithm.getMethod());
    } else if (S3AEncryptionMethods.SSE_KMS == algorithm) {
      copyObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
      // Set the KMS key if present, else S3 uses AWS managed key.
      EncryptionSecretOperations.getSSEAwsKMSKey(encryptionSecrets)
          .ifPresent(kmsKey -> copyObjectRequestBuilder.ssekmsKeyId(kmsKey));
    } else if (S3AEncryptionMethods.SSE_C == algorithm) {
      String base64customerKey = EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets);

      copyObjectRequestBuilder
          .copySourceSSECustomerAlgorithm(ServerSideEncryption.AES256.name())
          .copySourceSSECustomerKey(base64customerKey)
          .copySourceSSECustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)))
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    }
  }
  /**
   * Create a putObject request.
   * Adds the ACL, storage class and metadata
   * @param putObjectRequestBuilder putObject request builder
   * @param key key of object
   * @param options options for the request, including headers
   * @return the request
   */
  @Override
  public PutObjectRequest newPutObjectRequest(
      PutObjectRequest.Builder putObjectRequestBuilder,
      String key,
      final PutObjectOptions options) {

    Preconditions.checkArgument(isNotEmpty(key), "Null/empty key");

    putObjectRequestBuilder.bucket(getBucket()).key(key);

    if (options != null) {
      putObjectRequestBuilder.metadata(options.getHeaders());
    }

    // TODO: CannedACL will be converted to V2's ObjectCannedACL during MPU work.
    if (cannedACL != null) {
      putObjectRequestBuilder.acl(cannedACL.toString());
    }

    if (storageClass != null) {
      putObjectRequestBuilder.storageClass(storageClass.toString());
    }

    return putObjectRequestBuilder.build();
   // return prepareRequest(putObjectRequestBuilder);
  }

  @Override
  public PutObjectRequest.Builder buildPutObjectRequest(long length, boolean isDirectoryMarker) {

    PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder();

    if (length >= 0) {
      putObjectRequestBuilder.contentLength(length);
    }

    if (contentEncoding != null && !isDirectoryMarker) {
      putObjectRequestBuilder.contentEncoding(contentEncoding);
    }

    return putObjectRequestBuilder;
  }

  private void putEncryptionParameters(PutObjectRequest.Builder putObjectRequestBuilder) {
    final S3AEncryptionMethods algorithm
        = getServerSideEncryptionAlgorithm();

    if (S3AEncryptionMethods.SSE_S3 == algorithm) {
      putObjectRequestBuilder.serverSideEncryption(algorithm.getMethod());
    } else if (S3AEncryptionMethods.SSE_KMS == algorithm) {
      putObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
      // Set the KMS key if present, else S3 uses AWS managed key.
      EncryptionSecretOperations.getSSEAwsKMSKey(encryptionSecrets)
          .ifPresent(kmsKey -> putObjectRequestBuilder.ssekmsKeyId(kmsKey));
    } else if (S3AEncryptionMethods.SSE_C == algorithm) {
      String base64customerKey = EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets);

      putObjectRequestBuilder
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    }
  }

  @Override
  public PutObjectRequest newDirectoryMarkerRequest(String directory) {
    String key = directory.endsWith("/")
        ? directory
        : (directory + "/");
    // preparation happens in here
    PutObjectRequest.Builder putObjectRequestBuilder = buildPutObjectRequest(0L, true);

    putObjectRequestBuilder.contentType(HeaderProcessing.CONTENT_TYPE_X_DIRECTORY);

    PutObjectRequest putObjectRequest =
        newPutObjectRequest(putObjectRequestBuilder, key, null);
    return putObjectRequest;
  }

  @Override
  public ListMultipartUploadsRequest
      newListMultipartUploadsRequest(String prefix) {
    ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(
        getBucket());
    if (prefix != null) {
      request.setPrefix(prefix);
    }
    return prepareRequest(request);
  }

  @Override
  public AbortMultipartUploadRequest newAbortMultipartUploadRequest(
      String destKey,
      String uploadId) {
    return prepareRequest(new AbortMultipartUploadRequest(getBucket(),
        destKey,
        uploadId));
  }

  @Override
  public InitiateMultipartUploadRequest newMultipartUploadRequest(
      final String destKey,
      @Nullable final PutObjectOptions options) {
    // TODO: Temporarily removing metadata,
    //  will be added back in when this operation is updated.
    // final ObjectMetadata objectMetadata = newObjectMetadata(-1);
   // maybeSetMetadata(options, objectMetadata);
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(getBucket(),
            destKey);
    initiateMPURequest.setCannedACL(getCannedACL());
    if (getStorageClass() != null) {
      initiateMPURequest.withStorageClass(getStorageClass());
    }
    setOptionalMultipartUploadRequestParameters(initiateMPURequest);
    return prepareRequest(initiateMPURequest);
  }

  @Override
  public CompleteMultipartUploadRequest newCompleteMultipartUploadRequest(
      String destKey,
      String uploadId,
      List<PartETag> partETags) {
    // a copy of the list is required, so that the AWS SDK doesn't
    // attempt to sort an unmodifiable list.
    return prepareRequest(new CompleteMultipartUploadRequest(bucket,
        destKey, uploadId, new ArrayList<>(partETags)));

  }

  @Override
  public HeadObjectRequest newGetObjectMetadataRequest(String key) {
    String base64customerKey = EncryptionSecretOperations.getSSECustomerKey(encryptionSecrets);

    HeadObjectRequest.Builder request = HeadObjectRequest.builder()
        .bucket(getBucket())
        .key(key);

    // need to set key to get metadata for objects encrypted with SSE_C
    if (getServerSideEncryptionAlgorithm() == S3AEncryptionMethods.SSE_C) {
      request
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(base64customerKey)
          .sseCustomerKeyMD5(Md5Utils.md5AsBase64(Base64.getDecoder().decode(base64customerKey)));
    }

    return request.build();
  }

  @Override
  public GetObjectRequest newGetObjectRequest(String key) {
    GetObjectRequest request = new GetObjectRequest(bucket, key);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);

    return prepareRequest(request);
  }

  @Override
  public UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      int size,
      InputStream uploadStream,
      File sourceFile,
      long offset) throws PathIOException {
    checkNotNull(uploadId);
    // exactly one source must be set; xor verifies this
    checkArgument((uploadStream != null) ^ (sourceFile != null),
        "Data source");
    checkArgument(size >= 0, "Invalid partition size %s", size);
    checkArgument(partNumber > 0,
        "partNumber must be between 1 and %s inclusive, but is %s",
        DEFAULT_UPLOAD_PART_COUNT_LIMIT, partNumber);

    LOG.debug("Creating part upload request for {} #{} size {}",
        uploadId, partNumber, size);
    final String pathErrorMsg = "Number of parts in multipart upload exceeded."
        + " Current part count = %s, Part count limit = %s ";
    if (partNumber > multipartPartCountLimit) {
      throw new PathIOException(destKey,
          String.format(pathErrorMsg, partNumber, multipartPartCountLimit));
    }
    UploadPartRequest request = new UploadPartRequest()
        .withBucketName(getBucket())
        .withKey(destKey)
        .withUploadId(uploadId)
        .withPartNumber(partNumber)
        .withPartSize(size);
    if (uploadStream != null) {
      // there's an upload stream. Bind to it.
      request.setInputStream(uploadStream);
    } else {
      checkArgument(sourceFile.exists(),
          "Source file does not exist: %s", sourceFile);
      checkArgument(sourceFile.isFile(),
          "Source is not a file: %s", sourceFile);
      checkArgument(offset >= 0, "Invalid offset %s", offset);
      long length = sourceFile.length();
      checkArgument(offset == 0 || offset < length,
          "Offset %s beyond length of file %s", offset, length);
      request.setFile(sourceFile);
      request.setFileOffset(offset);
    }
    setOptionalUploadPartRequestParameters(request);
    return prepareRequest(request);
  }

  @Override
  public SelectObjectContentRequest newSelectRequest(String key) {
    SelectObjectContentRequest request = new SelectObjectContentRequest();
    request.setBucketName(bucket);
    request.setKey(key);
    generateSSECustomerKey().ifPresent(request::setSSECustomerKey);
    return prepareRequest(request);
  }

  @Override
  public ListObjectsRequest newListObjectsV1Request(
      final String key,
      final String delimiter,
      final int maxKeys) {

    ListObjectsRequest.Builder requestBuilder =
        ListObjectsRequest.builder().bucket(bucket).maxKeys(maxKeys).prefix(key);

    if (delimiter != null) {
      requestBuilder.delimiter(delimiter);
    }

    //TODO: add call to prepareRequest
    return requestBuilder.build();
  }

  @Override
  public ListNextBatchOfObjectsRequest newListNextBatchOfObjectsRequest(
      ObjectListing prev) {
    return prepareRequest(new ListNextBatchOfObjectsRequest(prev));
  }

  @Override
  public ListObjectsV2Request newListObjectsV2Request(
      final String key,
      final String delimiter,
      final int maxKeys) {

    final ListObjectsV2Request.Builder requestBuilder =
        ListObjectsV2Request.builder().bucket(bucket).maxKeys(maxKeys).prefix(key);

    if (delimiter != null) {
      requestBuilder.delimiter(delimiter);
    }

    //TODO: add call to prepareRequest, not added for now as PrepareRequest is a functional
    // interface, uses AmazonWebServiceRequest, SDKV2 uses AwsRequest.
    return requestBuilder.build();
  }

  @Override
  public DeleteObjectRequest newDeleteObjectRequest(String key) {
    return prepareRequest(new DeleteObjectRequest(bucket, key));
  }

  @Override
  public DeleteObjectsRequest newBulkDeleteRequest(
          List<DeleteObjectsRequest.KeyVersion> keysToDelete) {
    return prepareRequest(
        new DeleteObjectsRequest(bucket)
            .withKeys(keysToDelete)
            .withQuiet(true));
  }

  @Override
  public void setEncryptionSecrets(final EncryptionSecrets secrets) {
    encryptionSecrets = secrets;
  }

  /**
   * Create a builder.
   * @return new builder.
   */
  public static RequestFactoryBuilder builder() {
    return new RequestFactoryBuilder();
  }

  /**
   * Builder.
   */
  public static final class RequestFactoryBuilder {

    /**
     * Target bucket.
     */
    private String bucket;

    /**
     * Encryption secrets.
     */
    private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();

    /**
     * ACL For new objects.
     */
    private CannedAccessControlList cannedACL = null;

    /** Content Encoding. */
    private String contentEncoding;

    /**
     * Storage class.
     */
    private StorageClass storageClass;

    /**
     * Multipart limit.
     */
    private long multipartPartCountLimit = DEFAULT_UPLOAD_PART_COUNT_LIMIT;

    /**
     * Callback to prepare requests.
     */
    private PrepareRequest requestPreparer;

    private RequestFactoryBuilder() {
    }

    /**
     * Build the request factory.
     * @return the factory
     */
    public RequestFactory build() {
      return new RequestFactoryImpl(this);
    }

    /**
     * Content encoding.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withContentEncoding(final String value) {
      contentEncoding = value;
      return this;
    }

    /**
     * Storage class.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withStorageClass(final StorageClass value) {
      storageClass = value;
      return this;
    }

    /**
     * Target bucket.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withBucket(final String value) {
      bucket = value;
      return this;
    }

    /**
     * Encryption secrets.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withEncryptionSecrets(
        final EncryptionSecrets value) {
      encryptionSecrets = value;
      return this;
    }

    /**
     * ACL For new objects.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withCannedACL(
        final CannedAccessControlList value) {
      cannedACL = value;
      return this;
    }

    /**
     * Multipart limit.
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withMultipartPartCountLimit(
        final long value) {
      multipartPartCountLimit = value;
      return this;
    }

    /**
     * Callback to prepare requests.
     *
     * @param value new value
     * @return the builder
     */
    public RequestFactoryBuilder withRequestPreparer(
        final PrepareRequest value) {
      this.requestPreparer = value;
      return this;
    }
  }

  /**
   * This is a callback for anything to "prepare" every request
   * after creation. The S3AFileSystem's Audit Manager is expected
   * to be wired up via this call so can audit/prepare requests
   * after their creation.
   */
  @FunctionalInterface
  public interface PrepareRequest {

    /**
     * Post-creation preparation of AWS request.
     * @param t request
     * @param <T> request type.
     * @return prepared entry.
     */
    @Retries.OnceRaw
    <T extends AmazonWebServiceRequest> T prepareRequest(T t);
  }
}
