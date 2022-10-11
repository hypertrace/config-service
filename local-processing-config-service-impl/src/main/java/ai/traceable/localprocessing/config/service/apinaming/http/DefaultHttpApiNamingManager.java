package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingCachedConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpCustomApiNamingRulesManager;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.HttpApiNamingTrieManager;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.LocalApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import com.google.inject.Inject;
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultHttpApiNamingManager implements HttpApiNamingManager {

  private final HttpApiNamingConfigManager httpApiNamingConfigManager;
  private final HttpApiNamingTrieManager httpApiNamingTrieManager;
  private final HttpApiNamingCachedConfigManager httpApiNamingCachedConfigManager;
  private final HttpCustomApiNamingRulesManager httpCustomApiNamingRulesManager;
  private final EntityFetcher entityFetcher;
  private final LocalApiNamingConfigManager localApiNamingConfigManager;

  @Inject
  public DefaultHttpApiNamingManager(
      HttpApiNamingConfigManager httpApiNamingConfigManager,
      HttpApiNamingTrieManager httpApiNamingTrieManager,
      HttpApiNamingCachedConfigManager httpApiNamingCachedConfigManager,
      HttpCustomApiNamingRulesManager httpCustomApiNamingRulesManager,
      LocalApiNamingConfigManager localApiNamingConfigManager,
      EntityFetcher entityFetcher) {
    this.httpApiNamingConfigManager = httpApiNamingConfigManager;
    this.httpApiNamingTrieManager = httpApiNamingTrieManager;
    this.httpApiNamingCachedConfigManager = httpApiNamingCachedConfigManager;
    this.httpCustomApiNamingRulesManager = httpCustomApiNamingRulesManager;
    this.localApiNamingConfigManager = localApiNamingConfigManager;
    this.entityFetcher = entityFetcher;
  }

  public Duration getAgentPollingFrequency() {
    java.time.Duration agentPollingFrequency =
        localApiNamingConfigManager.getAgentPollingFrequency();
    return Duration.newBuilder()
        .setSeconds(agentPollingFrequency.getSeconds())
        .setNanos(agentPollingFrequency.getNano())
        .build();
  }

  public List<HttpServiceResponse> getHttpServiceResponseList(
      RequestContext requestContext, GetApiNamingModelRequest request) throws ExecutionException {
    Optional<String> maybeEnvironment = getEnvironment(request);
    Map<ServiceRequest, Optional<String>> serviceIdServiceRequestMap =
        entityFetcher.getServiceIds(
            requestContext, request.getServiceRequestsList(), maybeEnvironment);
    return buildHttpResponses(request, requestContext, serviceIdServiceRequestMap);
  }

  private List<HttpServiceResponse> buildHttpResponses(
      GetApiNamingModelRequest request,
      RequestContext requestContext,
      Map<ServiceRequest, Optional<String>> serviceIdServiceRequestMap) {
    List<HttpServiceResponse> httpServiceResponses = new ArrayList<>();
    Optional<String> maybeEnvironment = getEnvironment(request);
    serviceIdServiceRequestMap.forEach(
        (serviceRequest, serviceIdMaybe) -> {
          try {
            if (serviceIdMaybe.isEmpty()) {
              httpServiceResponses.add(
                  HttpServiceResponse.newBuilder()
                      .setServiceName(serviceRequest.getServiceName())
                      .build());
            } else {
              String serviceId = serviceIdMaybe.get();
              httpApiNamingCachedConfigManager
                  .getTrainingConfigListForService(requestContext, serviceId)
                  .ifPresent(
                      trainingConfigs -> {
                        try {
                          LocalApiNamingConfigInfo localApiNamingConfigInfo =
                              localApiNamingConfigManager.getLocalApiNamingConfigInfo(
                                  requestContext, serviceId);
                          if (!localApiNamingConfigInfo.isDisabled()) {
                            List<ApiNamingRule> apiNamingRules =
                                httpCustomApiNamingRulesManager.getApiNamingRules(
                                    requestContext,
                                    serviceRequest.getServiceName(),
                                    maybeEnvironment);
                            HttpApiNamingConfigInfo httpApiNamingConfigInfo =
                                httpApiNamingConfigManager.getHttpApiNamingConfigInfo(
                                    trainingConfigs,
                                    apiNamingRules,
                                    serviceRequest.getConfigHash());
                            httpServiceResponses.add(
                                HttpServiceResponse.newBuilder()
                                    .setServiceName(serviceRequest.getServiceName())
                                    .setApiNamingPatterns(
                                        httpApiNamingTrieManager.getApiNamingPatterns(
                                            requestContext,
                                            httpApiNamingConfigInfo,
                                            serviceId,
                                            serviceRequest.hasToken()
                                                ? serviceRequest.getToken()
                                                : "t=0;v=0.0.0",
                                            localApiNamingConfigInfo.getVersion()))
                                    .setHttpConfig(httpApiNamingConfigInfo.getHttpApiNamingConfig())
                                    .build());
                          }
                        } catch (Exception e) {
                          log.error("Could not retrieve trie for request:{}", request, e);
                        }
                      });
            }
          } catch (ExecutionException e) {
            log.error("Could not fetch training config for request:{}", request, e);
          }
        });
    return Collections.unmodifiableList(httpServiceResponses);
  }

  private Optional<String> getEnvironment(GetApiNamingModelRequest request) {
    if (request.hasEnvironment()) {
      return Optional.of(request.getEnvironment());
    }
    return Optional.empty();
  }
}
