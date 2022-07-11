package ai.traceable.external.userattribution.config.service;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionConfigServiceGrpc.ExternalUserAttributionConfigServiceImplBase;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import io.grpc.stub.StreamObserver;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

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
      ExternalUserAttributionRules rules =
          this.ruleTranslator.translateRules(
              this.userAttributionRuleStub
                  .getUserAttributionRules(GetUserAttributionRulesRequest.getDefaultInstance())
                  .getRulesList()
                  .stream()
                  .filter(rule -> !rule.getDisabled())
                  .collect(Collectors.toUnmodifiableList()));
      responseObserver.onNext(this.responseBuilder.buildResponse(request, rules));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Unable to get rules", exception);
      responseObserver.onError(exception);
    }
  }
}
