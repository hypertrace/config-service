package ai.traceable.external.data.classification.config.service.session;

import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.EnvironmentFilter;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest.GetSessionIdentificationRulesFilter;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SessionIdentificationRulesDao {
  private final SessionIdentificationConfigServiceGrpc
          .SessionIdentificationConfigServiceBlockingStub
      sessionIdentificationConfigServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  public SessionIdentificationRulesDao(
      SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceBlockingStub
          sessionIdentificationConfigServiceBlockingStub,
      ClientConfig clientConfig) {
    this.sessionIdentificationConfigServiceBlockingStub =
        sessionIdentificationConfigServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  public List<SessionIdentificationRule> getEnabledSessionIdentificationRules(
      RequestContext requestContext, EnvironmentFilter environmentFilter) {
    List<String> matchingEnvironments =
        Optional.of(environmentFilter.getEnvironmentName())
            .filter(Predicate.not(String::isBlank))
            .map(List::of)
            .orElseGet(Collections::emptyList); // no matching envs means rules with no scoped env
    return requestContext.call(
        () ->
            sessionIdentificationConfigServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getSessionIdentificationRules(
                    GetSessionIdentificationRulesRequest.newBuilder()
                        .setFilter(
                            GetSessionIdentificationRulesFilter.newBuilder()
                                .setDisabled(false)
                                .setEnvironmentFilter(
                                    GetSessionIdentificationRulesFilter.EnvironmentFilter
                                        .newBuilder()
                                        .addAllEnvironmentNames(matchingEnvironments)))
                        .build())
                .getRulesList()
                .stream()
                .collect(Collectors.toUnmodifiableList()));
  }
}
