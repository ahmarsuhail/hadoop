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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.RuntimeHttpUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static com.amazonaws.services.s3.Headers.REQUESTER_PAYS_HEADER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.BUCKET_REGION_HEADER;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_301_MOVED_PERMANENTLY;

/**
 * The default {@link S3ClientFactory} implementation.
 * This calls the AWS SDK to configure and create an
 * {@code AmazonS3Client} that communicates with the S3 service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultS3ClientFactory extends Configured
    implements S3ClientFactory {

  private static final String S3_SERVICE_NAME = "s3";

  private static final String REQUESTER_PAYS_HEADER_VALUE = "requester";

  /**
   * Subclasses refer to this.
   */
  protected static final Logger LOG =
      LoggerFactory.getLogger(DefaultS3ClientFactory.class);

  /**
   * A one-off warning of default region chains in use.
   */
  private static final LogExactlyOnce WARN_OF_DEFAULT_REGION_CHAIN =
      new LogExactlyOnce(LOG);

  /**
   * Warning message printed when the SDK Region chain is in use.
   */
  private static final String SDK_REGION_CHAIN_IN_USE =
      "S3A filesystem client is using"
          + " the SDK region resolution chain.";

  /** Exactly once log to inform about ignoring the AWS-SDK Warnings for CSE. */
  private static final LogExactlyOnce IGNORE_CSE_WARN = new LogExactlyOnce(LOG);

  /** Bucket name. */
  private String bucket;

  @Override
  public S3Client createS3Client(
      final URI uri,
      final S3ClientCreationParameters parameters) throws IOException {

    Configuration conf = getConf();
    bucket = uri.getHost();

    // TODO: Once the S3 Encryption client is available in SDK v2, check if CSE is enabled and
    //  create the encryption client.

    ApacheHttpClient.Builder httpClientBuilder = AWSClientConfig
        .createHttpClientBuilder(conf)
        .proxyConfiguration(AWSClientConfig.createProxyConfiguration(conf, bucket));
    return configureClientBuilder(S3Client.builder(), parameters, conf, bucket)
        .httpClientBuilder(httpClientBuilder)
        .build();
  }

  @Override
  public S3AsyncClient createS3AsyncClient(
      final URI uri,
      final S3ClientCreationParameters parameters) throws IOException {

    Configuration conf = getConf();
    bucket = uri.getHost();
    NettyNioAsyncHttpClient.Builder httpClientBuilder = AWSClientConfig
        .createAsyncHttpClientBuilder(conf)
        .proxyConfiguration(AWSClientConfig.createAsyncProxyConfiguration(conf, bucket));
    return configureClientBuilder(S3AsyncClient.builder(), parameters, conf, bucket)
        .httpClientBuilder(httpClientBuilder)
        .build();
  }

  /**
   * Configure a sync or async S3 client builder.
   * This method handles all shared configuration.
   * @param builder S3 client builder
   * @param parameters parameter object
   * @param conf configuration object
   * @param bucket bucket name
   * @return the builder object
   * @param <BuilderT> S3 client builder type
   * @param <ClientT> S3 client type
   */
  private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT>
  BuilderT configureClientBuilder(
      BuilderT builder,
      S3ClientCreationParameters parameters,
      Configuration conf,
      String bucket) {

    URI endpoint = getS3Endpoint(parameters.getEndpoint(), conf);
    Region region = getS3Region(conf.getTrimmed(AWS_REGION), bucket,
        parameters.getCredentialSet());
    LOG.debug("Using endpoint {}; and region {}", endpoint, region);

    // TODO: Some configuration done in configureBasicParams is not done yet.
    S3Configuration serviceConfiguration = S3Configuration.builder()
        .pathStyleAccessEnabled(parameters.isPathStyleAccess())
        // TODO: Review. Currently required to pass access point tests in ITestS3ABucketExistence,
        //  but resolving the region from the ap may be the correct solution.
        .useArnRegionEnabled(true)
        .build();

    return builder
        .overrideConfiguration(createClientOverrideConfiguration(parameters, conf))
        .credentialsProvider(parameters.getCredentialSet())
        .endpointOverride(endpoint)
        .region(region)
        .serviceConfiguration(serviceConfiguration);
  }

  /**
   * Create an override configuration for an S3 client.
   * @param parameters parameter object
   * @param conf configuration object
   * @return the override configuration
   */
  protected ClientOverrideConfiguration createClientOverrideConfiguration(
      S3ClientCreationParameters parameters, Configuration conf) {
    final ClientOverrideConfiguration.Builder clientOverrideConfigBuilder =
        AWSClientConfig.createClientConfigBuilder(conf);

    // add any headers
    parameters.getHeaders().forEach((h, v) -> clientOverrideConfigBuilder.putHeader(h, v));

    if (parameters.isRequesterPays()) {
      // All calls must acknowledge requester will pay via header.
      clientOverrideConfigBuilder.putHeader(REQUESTER_PAYS_HEADER, REQUESTER_PAYS_HEADER_VALUE);
    }

    if (!StringUtils.isEmpty(parameters.getUserAgentSuffix())) {
      clientOverrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX,
          parameters.getUserAgentSuffix());
    }

    if (parameters.getExecutionInterceptors() != null) {
      for (ExecutionInterceptor interceptor : parameters.getExecutionInterceptors()) {
        clientOverrideConfigBuilder.addExecutionInterceptor(interceptor);
      }
    }

    if (parameters.getMetrics() != null) {
      clientOverrideConfigBuilder.addMetricPublisher(
          new AwsStatisticsCollector(parameters.getMetrics()));
    }

    final RetryPolicy.Builder retryPolicyBuilder = AWSClientConfig.createRetryPolicyBuilder(conf);
    clientOverrideConfigBuilder.retryPolicy(retryPolicyBuilder.build());

    return clientOverrideConfigBuilder.build();
  }

  /**
   * Configure classic S3 client.
   * <p>
   * This includes: endpoint, Path Access and possibly other
   * options.
   *
   * @param s3 S3 Client.
   * @param endPoint s3 endpoint, may be empty
   * @param pathStyleAccess enable path style access?
   * @return S3 client
   * @throws IllegalArgumentException if misconfigured
   */
  protected static AmazonS3 configureAmazonS3Client(AmazonS3 s3,
      final String endPoint,
      final boolean pathStyleAccess)
      throws IllegalArgumentException {
    if (!endPoint.isEmpty()) {
      try {
        s3.setEndpoint(endPoint);
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
    }
    if (pathStyleAccess) {
      LOG.debug("Enabling path style access!");
      s3.setS3ClientOptions(S3ClientOptions.builder()
          .setPathStyleAccess(true)
          .build());
    }
    return s3;
  }

  /**
   * Given an endpoint string, return an endpoint config, or null, if none
   * is needed.
   * <p>
   * This is a pretty painful piece of code. It is trying to replicate
   * what AwsClient.setEndpoint() does, because you can't
   * call that setter on an AwsClient constructed via
   * the builder, and you can't pass a metrics collector
   * down except through the builder.
   * <p>
   * Note also that AWS signing is a mystery which nobody fully
   * understands, especially given all problems surface in a
   * "400 bad request" response, which, like all security systems,
   * provides minimal diagnostics out of fear of leaking
   * secrets.
   *
   * @param endpoint possibly null endpoint.
   * @param awsConf config to build the URI from.
   * @param awsRegion AWS S3 Region if the corresponding config is set.
   * @return a configuration for the S3 client builder.
   */
  @VisibleForTesting
  public static AwsClientBuilder.EndpointConfiguration
      createEndpointConfiguration(
      final String endpoint, final ClientConfiguration awsConf,
      String awsRegion) {
    LOG.debug("Creating endpoint configuration for \"{}\"", endpoint);
    if (endpoint == null || endpoint.isEmpty()) {
      // the default endpoint...we should be using null at this point.
      LOG.debug("Using default endpoint -no need to generate a configuration");
      return null;
    }

    final URI epr = RuntimeHttpUtils.toUri(endpoint, awsConf);
    LOG.debug("Endpoint URI = {}", epr);
    String region = awsRegion;
    if (StringUtils.isBlank(region)) {
      if (!ServiceUtils.isS3USStandardEndpoint(endpoint)) {
        LOG.debug("Endpoint {} is not the default; parsing", epr);
        region = AwsHostNameUtils.parseRegion(
            epr.getHost(),
            S3_SERVICE_NAME);
      } else {
        // US-east, set region == null.
        LOG.debug("Endpoint {} is the standard one; declare region as null",
            epr);
        region = null;
      }
    }
    LOG.debug("Region for endpoint {}, URI {} is determined as {}",
        endpoint, epr, region);
    return new AwsClientBuilder.EndpointConfiguration(endpoint, region);
  }

  /**
   * Given a endpoint string, create the endpoint URI.
   *
   * @param endpoint possibly null endpoint.
   * @param conf config to build the URI from.
   * @return an endpoint uri
   */
  private static URI getS3Endpoint(String endpoint, final Configuration conf) {

    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);

    String protocol = secureConnections ? "https" : "http";

    if (endpoint == null || endpoint.isEmpty()) {
      // the default endpoint
      endpoint = CENTRAL_ENDPOINT;
    }

    if (!endpoint.contains("://")) {
      endpoint = String.format("%s://%s", protocol, endpoint);
    }

    try {
      return new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Get the bucket region.
   *
   * @param region AWS S3 Region set in the config. This property may not be set, in which case
   *               ask S3 for the region.
   * @param bucket Bucket name.
   * @param credentialsProvider Credentials provider to be used with the default s3 client.
   * @return region of the bucket.
   */
  private static Region getS3Region(String region, String bucket,
      AwsCredentialsProvider credentialsProvider) {

    if (!StringUtils.isBlank(region)) {
      return Region.of(region);
    }

    try {
      // build a s3 client with region eu-west-1 that can be used to get the region of the bucket.
      // Using eu-west-1, as headBucket() doesn't work with us-east-1. This is because
      // us-east-1 uses the endpoint s3.amazonaws.com, which resolves bucket.s3.amazonaws.com to
      // the actual region the bucket is in. As the request is signed with us-east-1 and not the
      // bucket's region, it fails.
      S3Client s3Client = S3Client.builder().region(Region.EU_WEST_1)
          .credentialsProvider(credentialsProvider)
          .build();

      HeadBucketResponse headBucketResponse =
          s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
      return Region.of(
          headBucketResponse.sdkHttpResponse().headers().get(BUCKET_REGION_HEADER).get(0));
    } catch (S3Exception exception) {
      if (exception.statusCode() == SC_301_MOVED_PERMANENTLY) {
        List<String> bucketRegion =
            exception.awsErrorDetails().sdkHttpResponse().headers().get(BUCKET_REGION_HEADER);
        return Region.of(bucketRegion.get(0));
      }
    }

    return Region.US_EAST_1;
  }
}
