package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingCachedConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.HttpApiNamingTrieManager;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.LocalApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;

@Slf4j
class DefaultHttpApiNamingManager implements HttpApiNamingManager {

  private final HttpApiNamingConfigManager httpApiNamingConfigManager;
  private final HttpApiNamingTrieManager httpApiNamingTrieManager;
  private final HttpApiNamingCachedConfigManager httpApiNamingCachedConfigManager;
  private final EntityFetcher entityFetcher;
  private final LocalApiNamingConfigManager localApiNamingConfigManager;

  @Inject
  public DefaultHttpApiNamingManager(
      HttpApiNamingConfigManager httpApiNamingConfigManager,
      HttpApiNamingTrieManager httpApiNamingTrieManager,
      HttpApiNamingCachedConfigManager httpApiNamingCachedConfigManager,
      LocalApiNamingConfigManager localApiNamingConfigManager,
      EntityFetcher entityFetcher) {
    this.httpApiNamingConfigManager = httpApiNamingConfigManager;
    this.httpApiNamingTrieManager = httpApiNamingTrieManager;
    this.httpApiNamingCachedConfigManager = httpApiNamingCachedConfigManager;
    this.localApiNamingConfigManager = localApiNamingConfigManager;
    this.entityFetcher = entityFetcher;
  }

  public List<HttpServiceResponse> getHttpServiceResponseList(
      RequestContext requestContext, GetApiNamingModelRequest request) {
    Map<String, ServiceRequest> serviceIdServiceRequestMap = new HashMap<>();
    Optional<String> maybeEnvironment = getEnvironment(request);
    for (ServiceRequest serviceRequest : request.getServiceRequestsList()) {

      Optional<Entity> entityMaybe =
          entityFetcher.getEntity(
              requestContext, serviceRequest.getServiceName(), maybeEnvironment);
      if (entityMaybe.isEmpty()) {
        log.error(
            "Could not fetch entity for tenant id:{} and service request : {}",
            requestContext.getTenantId(),
            serviceRequest);
        continue;
      }
      Entity entity = entityMaybe.get();

      if (!isValidEntity(entity)) {
        log.error(
            "Entity not found for tenant id:{} and service request : {}",
            requestContext.getTenantId(),
            serviceRequest);
        continue;
      }
      serviceIdServiceRequestMap.put(entity.getEntityId(), serviceRequest);
    }

    return buildHttpResponses(request, requestContext, serviceIdServiceRequestMap);
  }

  private List<HttpServiceResponse> buildHttpResponses(
      GetApiNamingModelRequest request,
      RequestContext requestContext,
      Map<String, ServiceRequest> serviceIdServiceRequestMap) {
    List<HttpServiceResponse> httpServiceResponses = new ArrayList<>();
    serviceIdServiceRequestMap.forEach(
        (serviceId, serviceRequest) -> {
          try {
            httpApiNamingCachedConfigManager
                .getTrainingConfigListForService(requestContext, serviceId)
                .ifPresent(
                    trainingConfigs -> {
                      try {
                        LocalApiNamingConfigInfo localApiNamingConfigInfo =
                            localApiNamingConfigManager.getLocalApiNamingConfigInfo(
                                requestContext, serviceId);
                        if (!localApiNamingConfigInfo.isDisabled()) {
                          HttpApiNamingConfigInfo httpApiNamingConfigInfo =
                              httpApiNamingConfigManager.getHttpApiNamingConfigInfo(
                                  trainingConfigs, serviceRequest.getConfigHash());
                          httpServiceResponses.add(
                              HttpServiceResponse.newBuilder()
                                  .setServiceName(serviceRequest.getServiceName())
                                  .setTrie(
                                      httpApiNamingTrieManager.getTrie(
                                          requestContext,
                                          httpApiNamingConfigInfo,
                                          serviceId,
                                          serviceRequest.hasTrieToken()
                                              ? serviceRequest.getTrieToken()
                                              : "0",
                                          localApiNamingConfigInfo.getTimestamp()))
                                  .setHttpConfig(httpApiNamingConfigInfo.getHttpApiNamingConfig())
                                  .build());
                        }
                      } catch (Exception e) {
                        log.error("Could not retrieve trie for request:{}", request, e);
                      }
                    });
          } catch (ExecutionException e) {
            log.error("Could not fetch training config for request:{}", request, e);
          }
        });
    return Collections.unmodifiableList(httpServiceResponses);
  }

  private boolean isValidEntity(Entity entity) {
    return entity != null && !entity.getEntityId().isEmpty();
  }

  private Optional<String> getEnvironment(GetApiNamingModelRequest request) {
    if (request.hasEnvironment()) {
      return Optional.of(request.getEnvironment());
    }
    return Optional.empty();
  }
}
