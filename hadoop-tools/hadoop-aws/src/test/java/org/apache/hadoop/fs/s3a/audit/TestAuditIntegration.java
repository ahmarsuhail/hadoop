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

package org.apache.hadoop.fs.s3a.audit;

import java.io.IOException;
import java.nio.file.AccessDeniedException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditor;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import software.amazon.awssdk.core.interceptor.ExecutionAttributes;

import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.audit.AuditIntegration.attachSpanToRequest;
import static org.apache.hadoop.fs.s3a.audit.AuditIntegration.retrieveAttachedSpan;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.createIOStatisticsStoreForAuditing;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.noopAuditConfig;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_SERVICE_CLASSNAME;
import static org.apache.hadoop.service.ServiceAssert.assertServiceStateStarted;
import static org.apache.hadoop.service.ServiceAssert.assertServiceStateStopped;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for auditing.
 */
public class TestAuditIntegration extends AbstractHadoopTestBase {

  private final IOStatisticsStore ioStatistics =
      createIOStatisticsStoreForAuditing();

  /**
   * AuditFailureException is mapped to AccessDeniedException.
   */
  @Test
  public void testExceptionTranslation() throws Throwable {
    intercept(AccessDeniedException.class,
        () -> {
          throw translateException("test", "/",
              new AuditFailureException("should be translated"));
        });
  }

  /**
   * Create a no-op auditor.
   */
  @Test
  public void testNoOpAuditorInstantiation() throws Throwable {
    OperationAuditor auditor = createAndStartNoopAuditor(
        ioStatistics);
    assertThat(auditor)
        .describedAs("No-op auditor")
        .isInstanceOf(NoopAuditor.class)
        .satisfies(o -> o.isInState(Service.STATE.STARTED));
  }

  /**
   * Create a no-op auditor through AuditIntegration, just as
   * the audit manager does.
   * @param store stats store.
   * @return a started auditor
   */
  private NoopAuditor createAndStartNoopAuditor(
      final IOStatisticsStore store)
      throws IOException {
    Configuration conf = noopAuditConfig();
    OperationAuditorOptions options =
        OperationAuditorOptions.builder()
            .withConfiguration(conf)
            .withIoStatisticsStore(store);
    OperationAuditor auditor =
        AuditIntegration.createAndInitAuditor(conf,
            AUDIT_SERVICE_CLASSNAME,
            options);
    assertThat(auditor)
        .describedAs("No-op auditor")
        .isInstanceOf(NoopAuditor.class)
        .satisfies(o -> o.isInState(Service.STATE.INITED));
    auditor.start();
    return (NoopAuditor) auditor;
  }

  /**
   * The auditor class has to exist.
   */
  @Test
  public void testCreateNonexistentAuditor() throws Throwable {
    final Configuration conf = new Configuration();
    OperationAuditorOptions options =
        OperationAuditorOptions.builder()
            .withConfiguration(conf)
            .withIoStatisticsStore(ioStatistics);
    conf.set(AUDIT_SERVICE_CLASSNAME, "not.a.known.class");
    intercept(RuntimeException.class, () ->
        AuditIntegration.createAndInitAuditor(conf,
            AUDIT_SERVICE_CLASSNAME,
            options));
  }

  /**
   * The audit manager creates the auditor the config tells it to;
   * this will have the same lifecycle as the manager.
   */
  @Test
  public void testAuditManagerLifecycle() throws Throwable {
    AuditManagerS3A manager = AuditIntegration.createAndStartAuditManager(
        noopAuditConfig(),
        ioStatistics);
    OperationAuditor auditor = manager.getAuditor();
    assertServiceStateStarted(auditor);
    manager.close();
    assertServiceStateStopped(auditor);
  }

  // TODO: Temporarily commenting out as auditor still expects V1 request,
  //  will be updated during auditor work.
//  @Test
//  public void testSingleRequestHandler() throws Throwable {
//    AuditManagerS3A manager = AuditIntegration.createAndStartAuditManager(
//        noopAuditConfig(),
//        ioStatistics);
//    List<RequestHandler2> handlers
//        = manager.createRequestHandlers();
//    assertThat(handlers)
//        .hasSize(1);
//    RequestHandler2 handler = handlers.get(0);
//    RequestFactory requestFactory = RequestFactoryImpl.builder()
//        .withBucket("bucket")
//        .build();
//    // test the basic pre-request sequence while avoiding
//    // the complexity of recreating the full sequence
//    // (and probably getting it wrong)
//    GetObjectMetadataRequest r
//        = requestFactory.newGetObjectMetadataRequest("/");
//    DefaultRequest dr = new DefaultRequest(r, "S3");
//    assertThat(handler.beforeMarshalling(r))
//        .isNotNull();
//    assertThat(handler.beforeExecution(r))
//        .isNotNull();
//    handler.beforeRequest(dr);
//
//  }

  /**
   * Register a second handler, verify it makes it to the list.
   */
  @Test
  public void testRequestHandlerLoading() throws Throwable {
    Configuration conf = noopAuditConfig();
    conf.setClassLoader(this.getClass().getClassLoader());
    conf.set(AUDIT_EXECUTION_INTERCEPTORS,
        SimpleAWSExecutionInterceptor.CLASS);
    AuditManagerS3A manager = AuditIntegration.createAndStartAuditManager(
        conf,
        ioStatistics);
    assertThat(manager.createExecutionInterceptors())
        .hasSize(2)
        .hasAtLeastOneElementOfType(SimpleAWSExecutionInterceptor.class);
  }

  @Test
  public void testLoggingAuditorBinding() throws Throwable {
    AuditManagerS3A manager = AuditIntegration.createAndStartAuditManager(
        AuditTestSupport.loggingAuditConfig(),
        ioStatistics);
    OperationAuditor auditor = manager.getAuditor();
    assertServiceStateStarted(auditor);
    manager.close();
    assertServiceStateStopped(auditor);
  }

  @Test
  public void testNoopAuditManager() throws Throwable {
    AuditManagerS3A manager = AuditIntegration.stubAuditManager();
    assertThat(manager.createTransferListener())
        .describedAs("transfer listener")
        .isNotNull();
  }

  @Test
  public void testSpanAttachAndRetrieve() throws Throwable {
    AuditManagerS3A manager = AuditIntegration.stubAuditManager();

    AuditSpanS3A span = manager.createSpan("op", null, null);
    ExecutionAttributes attributes = ExecutionAttributes.builder().build();
    attachSpanToRequest(attributes, span);
    AuditSpanS3A retrievedSpan = retrieveAttachedSpan(attributes);
    assertThat(retrievedSpan).isSameAs(span);

  }
}
