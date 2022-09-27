package ai.traceable.external.agent.attribute.config.service;

import static java.util.concurrent.TimeUnit.SECONDS;

import ai.traceable.external.agent.attribute.config.service.translator.ExternalAgentAttributeRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRules;
import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc.ExternalAgentAttributeConfigServiceImplBase;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesResponse;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter.EnvironmentScopeFilter;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ExternalAgentAttributeConfigServiceImpl extends ExternalAgentAttributeConfigServiceImplBase {

  private static final int DEFAULT_DEADLINE_SECONDS = 10;

  private final UserAttributionConfigServiceBlockingStub userAttributionRuleStub;
  private final ExternalAgentAttributeRuleTranslator ruleTranslator;
  private final ExternalAgentAttributeRuleResponseBuilder responseBuilder;

  @Inject
  ExternalAgentAttributeConfigServiceImpl(
      UserAttributionConfigServiceBlockingStub userAttributionRuleStub,
      ExternalAgentAttributeRuleTranslator ruleTranslator,
      ExternalAgentAttributeRuleResponseBuilder responseBuilder) {
    this.userAttributionRuleStub = userAttributionRuleStub;
    this.ruleTranslator = ruleTranslator;
    this.responseBuilder = responseBuilder;
  }

  @Override
  public void getAgentAttributeRules(
      GetAgentAttributeRulesRequest request,
      StreamObserver<GetAgentAttributeRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      AgentAttributeRules rules =
          this.ruleTranslator.translateRules(
              fetchActiveUserAttributionRules(requestContext, request));
      responseObserver.onNext(this.responseBuilder.buildResponse(request, rules));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Unable to get rules", exception);
      responseObserver.onError(exception);
    }
  }

  private List<UserAttributionRule> fetchActiveUserAttributionRules(
      RequestContext requestContext, GetAgentAttributeRulesRequest request) {
    EnvironmentScopeFilter environmentScopeFilter;
    if (request.getScope().hasEnvironmentName()) {
      environmentScopeFilter =
          EnvironmentScopeFilter.newBuilder()
              .addEnvironmentNames(request.getScope().getEnvironmentName())
              .build();
    } else {
      environmentScopeFilter = EnvironmentScopeFilter.getDefaultInstance();
    }

    return requestContext.call(
        () ->
            userAttributionRuleStub
                .withDeadlineAfter(DEFAULT_DEADLINE_SECONDS, SECONDS)
                .getUserAttributionRules(
                    GetUserAttributionRulesRequest.newBuilder()
                        .setFilter(
                            GetUserAttributionRulesFilter.newBuilder()
                                .setScopeFilter(
                                    ScopeFilter.newBuilder()
                                        .setEnvironmentScopeFilter(environmentScopeFilter))
                                .setDisabled(false))
                        .build())
                .getRulesList()
                .stream()
                .collect(Collectors.toUnmodifiableList()));
  }
}
