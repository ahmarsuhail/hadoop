package org.apache.hadoop.fs.s3a.select;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

public class SelectHandler implements SelectObjectContentResponseHandler {
  private SelectObjectContentResponse response;
  public List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();
  private Throwable exception;

  @Override
  public void responseReceived(SelectObjectContentResponse response) {
    this.response = response;
  }

  @Override
  public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
    publisher.subscribe(receivedEvents::add);
  }

  @Override
  public void exceptionOccurred(Throwable throwable) {
    exception = throwable;
  }

  @Override
  public void complete() {
  }
}
