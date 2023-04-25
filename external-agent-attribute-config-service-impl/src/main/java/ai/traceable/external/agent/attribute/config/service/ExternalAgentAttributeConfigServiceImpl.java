package ai.traceable.external.agent.attribute.config.service;

import static ai.traceable.external.agent.attribute.config.service.ExternalAgentAttributeConfigServiceConstants.JWT_EXTRACTION_MIN_TPA_VERSION;
import static java.util.concurrent.TimeUnit.SECONDS;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc.AuthDetectionConfigServiceBlockingStub;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleFilter;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleScope;
import ai.traceable.auth.detection.config.service.v1.GetAuthDetectionRulesRequest;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.external.agent.attribute.config.service.translator.ExternalAgentAttributeRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc.ExternalAgentAttributeConfigServiceImplBase;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesResponse;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc.JwtExtractionConfigServiceBlockingStub;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleFilter;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleScope;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter.EnvironmentScopeFilter;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = @Inject)
class ExternalAgentAttributeConfigServiceImpl extends ExternalAgentAttributeConfigServiceImplBase {
  private static final int DEFAULT_DEADLINE_SECONDS = 10;

  private final UserAttributionConfigServiceBlockingStub userAttributionRuleStub;
  private final AuthDetectionConfigServiceBlockingStub authDetectionConfigServiceBlockingStub;
  private final JwtExtractionConfigServiceBlockingStub jwtExtractionBlockingStub;
  private final ExternalAgentAttributeRuleTranslator ruleTranslator;
  private final ExternalAgentAttributeRuleResponseBuilder responseBuilder;
  private final FeatureCachingClient featureCachingClient;
  private final SemanticVersioningComparator semanticVersioningComparator;

  @Override
  public void getAgentAttributeRules(
      GetAgentAttributeRulesRequest request,
      StreamObserver<GetAgentAttributeRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      if (!this.featureCachingClient.isUserAttributionV2Enabled(requestContext)) {
        responseObserver.onNext(this.responseBuilder.buildDisabledResponse());
        responseObserver.onCompleted();
        return;
      }
      List<AttributeRule> rules =
          this.ruleTranslator.translateRules(
              fetchActiveUserAttributionRules(requestContext, request),
              fetchAuthDetectionRules(requestContext, request),
              fetchActiveJwtAttributionRules(requestContext, request));
      responseObserver.onNext(this.responseBuilder.buildEnabledResponse(request, rules));
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
            List.copyOf(
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
                    .getRulesList()));
  }

  private List<AuthDetectionRule> fetchAuthDetectionRules(
      RequestContext requestContext, GetAgentAttributeRulesRequest request) {
    return requestContext.call(
        () ->
            List.copyOf(
                authDetectionConfigServiceBlockingStub
                    .withDeadlineAfter(DEFAULT_DEADLINE_SECONDS, SECONDS)
                    .getAuthDetectionRules(this.buildEquivalentAuthRuleRequest(request))
                    .getRulesList()));
  }

  private List<JwtExtractionRule> fetchActiveJwtAttributionRules(
      RequestContext requestContext, GetAgentAttributeRulesRequest request) {
    if (!semanticVersioningComparator.isVersionSupported(
        getTPAVersion(request.getAgentCapabilities()), JWT_EXTRACTION_MIN_TPA_VERSION)) {
      return Collections.emptyList();
    }
    JwtExtractionRuleFilter.Builder filterBuilder =
        JwtExtractionRuleFilter.newBuilder().setDisabled(false);
    if (request.getScope().hasEnvironmentName()) {
      filterBuilder.setScope(
          JwtExtractionRuleScope.newBuilder()
              .setEnvironmentScope(
                  JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                      .addEnvironmentNames(request.getScope().getEnvironmentName())));
    }
    return requestContext.call(
        () ->
            List.copyOf(
                jwtExtractionBlockingStub
                    .withDeadlineAfter(DEFAULT_DEADLINE_SECONDS, SECONDS)
                    .getJwtExtractionRules(
                        GetJwtExtractionRulesRequest.newBuilder()
                            .setFilter(filterBuilder.build())
                            .build())
                    .getRulesList()));
  }

  private GetAuthDetectionRulesRequest buildEquivalentAuthRuleRequest(
      GetAgentAttributeRulesRequest request) {
    if (!request.getScope().hasEnvironmentName()) {
      return GetAuthDetectionRulesRequest.getDefaultInstance();
    }

    return GetAuthDetectionRulesRequest.newBuilder()
        .setFilter(
            AuthDetectionRuleFilter.newBuilder()
                .setScope(
                    AuthDetectionRuleScope.newBuilder()
                        .addEnvironmentNames(request.getScope().getEnvironmentName())))
        .build();
  }

  private String getTPAVersion(GetAgentAttributeRulesRequest.AgentCapabilities agentCapabilities) {
    return agentCapabilities.getComponentsList().stream()
        .map(GetAgentAttributeRulesRequest.Component::getTraceablePlatformAgentVersion)
        .findFirst()
        .orElse("0.0.0");
  }
}
