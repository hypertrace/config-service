package ai.traceable.risk.config.service.v2.elements.builder;

import com.google.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.GetLabelsRequest;
import org.hypertrace.label.config.service.v1.Label;
import org.hypertrace.label.config.service.v1.LabelsConfigServiceGrpc.LabelsConfigServiceBlockingStub;

public class LabelsConfigProvider {

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(6);

  private final LabelsConfigServiceBlockingStub labelsConfigServiceBlockingStub;

  @Inject
  LabelsConfigProvider(LabelsConfigServiceBlockingStub labelsConfigServiceBlockingStub) {
    this.labelsConfigServiceBlockingStub = labelsConfigServiceBlockingStub;
  }

  public List<Label> getLabels(RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                labelsConfigServiceBlockingStub
                    .withDeadlineAfter(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .getLabels(GetLabelsRequest.getDefaultInstance()))
        .getLabelsList();
  }
}
