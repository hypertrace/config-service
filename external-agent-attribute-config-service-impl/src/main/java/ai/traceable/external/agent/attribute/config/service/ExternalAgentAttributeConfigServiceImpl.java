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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
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
  private final LoadingCache<ContextualKey<AgentAttributeIdentifier>, List<AttributeRule>>
      attributeRulesCache =
          CacheBuilder.newBuilder()
              .refreshAfterWrite(1, TimeUnit.MINUTES)
              .expireAfterAccess(5, TimeUnit.MINUTES)
              .maximumSize(10000)
              .recordStats()
              .build(
                  CacheLoader.asyncReloading(
                      CacheLoader.from(this::fetchAgentAttributeRules),
                      Executors.newSingleThreadExecutor()));

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
      ContextualKey<AgentAttributeIdentifier> agentAttributeIdentifier =
          requestContext.buildInternalContextualKey(
              new AgentAttributeIdentifier(
                  getEnvironmentName(request),
                  semanticVersioningComparator.isVersionSupported(
                      getTPAVersion(request.getAgentCapabilities()),
                      JWT_EXTRACTION_MIN_TPA_VERSION)));
      List<AttributeRule> rules = attributeRulesCache.getUnchecked(agentAttributeIdentifier);
      responseObserver.onNext(
          this.responseBuilder.buildEnabledResponse(
              request, ruleTranslator.condenseToSingleRule(rules)));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Unable to get rules", exception);
      responseObserver.onError(exception);
    }
  }

  private List<AttributeRule> fetchAgentAttributeRules(
      ContextualKey<AgentAttributeIdentifier> agentAttributeIdentifierContextualKey) {
    RequestContext requestContext = agentAttributeIdentifierContextualKey.getContext();
    Optional<String> environmentName =
        agentAttributeIdentifierContextualKey.getData().getEnvironmentName();
    boolean jwtExtractionSupported =
        agentAttributeIdentifierContextualKey.getData().isJwtExtractionSupported();
    return ruleTranslator.translateRules(
        fetchActiveUserAttributionRules(requestContext, environmentName),
        fetchAuthDetectionRules(requestContext, environmentName),
        jwtExtractionSupported
            ? fetchActiveJwtAttributionRules(requestContext, environmentName)
            : Collections.emptyList());
  }

  private List<UserAttributionRule> fetchActiveUserAttributionRules(
      RequestContext requestContext, Optional<String> environmentName) {
    EnvironmentScopeFilter environmentScopeFilter =
        environmentName
            .map(
                envName -> EnvironmentScopeFilter.newBuilder().addEnvironmentNames(envName).build())
            .orElse(EnvironmentScopeFilter.getDefaultInstance());
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
      RequestContext requestContext, Optional<String> environmentName) {
    GetAuthDetectionRulesRequest authDetectionRulesRequest =
        environmentName
            .map(
                envName ->
                    GetAuthDetectionRulesRequest.newBuilder()
                        .setFilter(
                            AuthDetectionRuleFilter.newBuilder()
                                .setScope(
                                    AuthDetectionRuleScope.newBuilder()
                                        .addEnvironmentNames(envName)))
                        .build())
            .orElse(GetAuthDetectionRulesRequest.getDefaultInstance());
    return requestContext.call(
        () ->
            List.copyOf(
                authDetectionConfigServiceBlockingStub
                    .withDeadlineAfter(DEFAULT_DEADLINE_SECONDS, SECONDS)
                    .getAuthDetectionRules(authDetectionRulesRequest)
                    .getRulesList()));
  }

  private List<JwtExtractionRule> fetchActiveJwtAttributionRules(
      RequestContext requestContext, Optional<String> environmentName) {
    JwtExtractionRuleFilter.Builder filterBuilder =
        JwtExtractionRuleFilter.newBuilder().setDisabled(false);
    environmentName.ifPresent(
        envName ->
            filterBuilder.setScope(
                JwtExtractionRuleScope.newBuilder()
                    .setEnvironmentScope(
                        JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                            .addEnvironmentNames(envName))));
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

  private Optional<String> getEnvironmentName(GetAgentAttributeRulesRequest request) {
    if (request.getScope().hasEnvironmentName()) {
      return Optional.of(request.getScope().getEnvironmentName());
    }
    return Optional.empty();
  }

  private String getTPAVersion(GetAgentAttributeRulesRequest.AgentCapabilities agentCapabilities) {
    return agentCapabilities.getComponentsList().stream()
        .map(GetAgentAttributeRulesRequest.Component::getTraceablePlatformAgentVersion)
        .findFirst()
        .orElse("0.0.0");
  }
}
