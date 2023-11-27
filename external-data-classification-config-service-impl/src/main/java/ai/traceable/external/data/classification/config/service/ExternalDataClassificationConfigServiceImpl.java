package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.external.data.classification.config.service.legacy.LegacyRuleManager;
import ai.traceable.external.data.classification.config.service.session.SessionIdentificationRulesDao;
import ai.traceable.external.data.classification.config.service.session.SessionIdentificationRulesTranslator;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceImplBase;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.OnlyIfChangedFilter;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
class ExternalDataClassificationConfigServiceImpl
    extends ExternalDataClassificationServiceImplBase {

  private final ExternalDataClassificationConfig externalDataClassificationConfig;
  private final ExternalDataClassificationConfigRequestValidator
      externalDataClassificationConfigRequestValidator;
  private final SessionIdentificationRulesDao sessionIdentificationRulesDao;
  private final SessionIdentificationRulesTranslator sessionIdentificationRulesTranslator;
  private final LegacyRuleManager legacyRuleManager;
  private final PlatformDataTypeManager platformDataTypeManager;
  private final OverrideRuleManager overrideRuleManager;
  private final DataParsingRuleManager dataParsingRuleManager;
  private final ExternalDataClassificationRuleResponseBuilder responseBuilder;
  private final FeatureCachingClient featureCachingClient;

  private final LoadingCache<
          ContextualKey<GetDataClassificationConfigRequest>, GetDataClassificationConfigResponse>
      responseCache;

  @Inject
  public ExternalDataClassificationConfigServiceImpl(
      ExternalDataClassificationConfig externalDataClassificationConfig,
      ExternalDataClassificationConfigRequestValidator
          externalDataClassificationConfigRequestValidator,
      SessionIdentificationRulesDao sessionIdentificationRulesDao,
      SessionIdentificationRulesTranslator sessionIdentificationRulesTranslator,
      LegacyRuleManager legacyRuleManager,
      PlatformDataTypeManager platformDataTypeManager,
      OverrideRuleManager overrideRuleManager,
      DataParsingRuleManager dataParsingRuleManager,
      ExternalDataClassificationRuleResponseBuilder responseBuilder,
      FeatureCachingClient featureCachingClient) {
    this.externalDataClassificationConfig = externalDataClassificationConfig;
    this.externalDataClassificationConfigRequestValidator =
        externalDataClassificationConfigRequestValidator;
    this.sessionIdentificationRulesDao = sessionIdentificationRulesDao;
    this.sessionIdentificationRulesTranslator = sessionIdentificationRulesTranslator;
    this.legacyRuleManager = legacyRuleManager;
    this.platformDataTypeManager = platformDataTypeManager;
    this.overrideRuleManager = overrideRuleManager;
    this.dataParsingRuleManager = dataParsingRuleManager;
    this.responseBuilder = responseBuilder;
    this.featureCachingClient = featureCachingClient;

    this.responseCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(this.externalDataClassificationConfig.getCacheExpirationDuration())
            .refreshAfterWrite(this.externalDataClassificationConfig.getCacheRefreshDuration())
            .maximumSize(this.externalDataClassificationConfig.getCacheMaxSize())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::calculateResponse), this.buildExecutor()));

    PlatformMetricsRegistry.registerCache(
        "ExternalDataClassificationConfigServiceImplResponseCache",
        this.responseCache,
        Collections.emptyMap());
  }

  @Override
  public void getDataClassificationConfig(
      GetDataClassificationConfigRequest request,
      StreamObserver<GetDataClassificationConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.externalDataClassificationConfigRequestValidator.validateOrThrow(
          requestContext, request);
      responseObserver.onNext(
          this.responseCache.get(requestContext.buildInternalContextualKey(request)));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get external data classification rules", e);
      responseObserver.onError(e);
    }
  }

  private GetDataClassificationConfigResponse calculateResponse(
      ContextualKey<GetDataClassificationConfigRequest> requestKey) {
    RequestContext requestContext = requestKey.getContext();
    if (!this.featureCachingClient.isDataClassificationRp2Enabled(requestContext)) {
      return this.responseBuilder.buildDisabledResponse();
    }
    GetDataClassificationConfigRequest request = requestKey.getData();
    Optional<String> requestedEnvironment =
        Optional.of(request.getEnvironmentFilter().getEnvironmentName())
            .filter(envName -> !envName.isBlank());
    List<DataClassificationOverride> overrides =
        overrideRuleManager.getOverrides(requestContext, request.getEnvironmentFilter());
    List<DataSet> enabledDataSets = this.platformDataTypeManager.getEnabledDataSets(requestContext);
    ImmutableList.Builder<DataType> customDataTypes = ImmutableList.builder();

    customDataTypes
        .addAll(
            this.legacyRuleManager.getDataTypesFromLegacyRedactionRules(
                requestContext, enabledDataSets))
        .addAll(
            this.platformDataTypeManager.getDataTypes(
                requestContext,
                enabledDataSets,
                requestedEnvironment,
                request.getPredicateSupportLevel()));
    if (featureCachingClient.isSessionIdentificationV2EnabledForTenant(requestContext)) {
      customDataTypes.addAll(
          this.sessionIdentificationRulesTranslator.translateSessionIdentificationRules(
              this.sessionIdentificationRulesDao.getEnabledSessionIdentificationRules(
                  requestContext, request.getEnvironmentFilter())));
    }
    customDataTypes.addAll(
        this.legacyRuleManager.getDataTypesFromLegacySensitiveHeaders(
            requestContext, enabledDataSets));

    List<DataType> externalDataTypes =
        ImmutableList.<DataType>builder()
            .addAll(
                this.overrideRuleManager.applyOverridesAndFilterRawRules(
                    requestContext, customDataTypes.build(), overrides))
            .addAll( // Default rules are always sent, even if they don't suppress (due to TPA bug)
                this.overrideRuleManager.applyOverrides(
                    requestContext,
                    this.platformDataTypeManager.getDefaultDataTypes(requestContext),
                    overrides))
            .build();

    // TODO - consider sorting the final list by redaction strategy. This would be a behavior change
    // But would prevent an obfuscating session rule overriding a redacting data classification rule
    GetDataClassificationConfigResponse response =
        this.responseBuilder.buildEnabledResponse(
            request,
            requestContext,
            externalDataTypes,
            this.dataParsingRuleManager.getDefaultParsingRulesForAgent(
                request.getAgentCapabilities()));
    this.updateCacheIfNeeded(requestKey, response);
    return response;
  }

  private void updateCacheIfNeeded(
      ContextualKey<GetDataClassificationConfigRequest> requestContextualKey,
      GetDataClassificationConfigResponse response) {
    // If the response has changed that will change the hash of the next request. However, since
    // request is effectively asking for the same data, we can calculate it and cache it, too.
    // For example, after the following call sequence:
    // { req, hash(old_resp) } -> { new_resp, hash(new_resp) }
    // We know the agent's next request should be
    // { req, hash(new_resp) }
    // And we can calculate the response to that, indicating nothing has changed should be
    // { empty_response, hash(new_resp) }
    GetDataClassificationConfigRequest lastRequest = requestContextualKey.getData();
    String newHash = response.getHash();
    if (lastRequest.getChangeFilter().getPreviousHash().equals(newHash)) {
      return;
    }
    GetDataClassificationConfigRequest expectedNextRequest =
        lastRequest.toBuilder()
            .setChangeFilter(OnlyIfChangedFilter.newBuilder().setPreviousHash(newHash).build())
            .build();
    this.responseCache.put(
        requestContextualKey.getContext().buildInternalContextualKey(expectedNextRequest),
        this.responseBuilder.buildNoChangeResponseForHash(newHash));
  }

  private ExecutorService buildExecutor() {
    return Executors.newFixedThreadPool(
        this.externalDataClassificationConfig.getCacheThreadPoolSize(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("external-data-classification-%d")
            .build());
  }
}
