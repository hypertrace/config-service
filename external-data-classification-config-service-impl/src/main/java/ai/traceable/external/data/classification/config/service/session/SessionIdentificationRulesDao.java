package ai.traceable.external.data.classification.config.service.session;

import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SessionIdentificationRulesDao {
  private final SessionIdentificationConfigServiceGrpc
          .SessionIdentificationConfigServiceBlockingStub
      sessionIdentificationConfigServiceBlockingStub;

  @Inject
  public SessionIdentificationRulesDao(
      SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceBlockingStub
          sessionIdentificationConfigServiceBlockingStub) {
    this.sessionIdentificationConfigServiceBlockingStub =
        sessionIdentificationConfigServiceBlockingStub;
  }

  public List<SessionIdentificationRule> getEnabledSessionIdentificationRules(
      RequestContext requestContext) {
    return requestContext.call(
        () ->
            sessionIdentificationConfigServiceBlockingStub
                .getSessionIdentificationRules(
                    GetSessionIdentificationRulesRequest.getDefaultInstance())
                .getRulesList()
                .stream()
                .filter(rule -> !rule.getStatus().getDisabled())
                .collect(Collectors.toUnmodifiableList()));
  }
}
