package ai.traceable.external.userattribution.config.service;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionConfigServiceGrpc.ExternalUserAttributionConfigServiceImplBase;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ExternalUserAttributionConfigServiceImpl
    extends ExternalUserAttributionConfigServiceImplBase {

  private final UserAttributionConfigServiceBlockingStub userAttributionRuleStub;
  private final ExternalUserAttributionRuleTranslator ruleTranslator;
  private final ExternalUserAttributionRuleResponseBuilder responseBuilder;

  @Inject
  ExternalUserAttributionConfigServiceImpl(
      UserAttributionConfigServiceBlockingStub userAttributionRuleStub,
      ExternalUserAttributionRuleTranslator ruleTranslator,
      ExternalUserAttributionRuleResponseBuilder responseBuilder) {
    this.userAttributionRuleStub = userAttributionRuleStub;
    this.ruleTranslator = ruleTranslator;
    this.responseBuilder = responseBuilder;
  }

  @Override
  public void getExternalUserAttributionRules(
      GetExternalUserAttributionRulesRequest request,
      StreamObserver<GetExternalUserAttributionRulesResponse> responseObserver) {

    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      ExternalUserAttributionRules rules =
          this.ruleTranslator.translateRules(fetchUserAttributionRules(requestContext, request));
      responseObserver.onNext(this.responseBuilder.buildResponse(request, rules));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Unable to get rules", exception);
      responseObserver.onError(exception);
    }
  }

  private List<UserAttributionRule> fetchUserAttributionRules(
      RequestContext requestContext, GetExternalUserAttributionRulesRequest request) {
    List<UserAttributionRule> userAttributionRules =
        requestContext.call(
            () ->
                userAttributionRuleStub
                    .getUserAttributionRules(GetUserAttributionRulesRequest.getDefaultInstance())
                    .getRulesList()
                    .stream()
                    .filter(rule -> !rule.getDisabled())
                    .collect(Collectors.toUnmodifiableList()));

    if (!request.hasEnvironmentName()) {
      return userAttributionRules;
    }

    return userAttributionRules.stream()
        .filter(
            userAttributionRule ->
                isApplicableToEnvironment(userAttributionRule, request.getEnvironmentName()))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean isApplicableToEnvironment(UserAttributionRule rule, String environmentName) {
    List<UserAttributionRuleScope.EnvironmentScope> environmentScopes =
        rule.getScope().getCustomScope().getEnvironmentScopesList();
    if (environmentScopes.isEmpty()) {
      return true;
    }
    return environmentScopes.stream()
        .anyMatch(
            environmentScope -> environmentScope.getEnvironmentName().equals(environmentName));
  }
}
