package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.CORE_MODE_RULE_CATEGORY;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub;
import ai.traceable.featureflag.v1.FeatureFlagValue;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesRequest;
import ai.traceable.sensitivedata.config.service.v1.FullPrivacyModeConfig;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest.RedactionRuleFilter;
import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.github.f4b6a3.uuid.util.UuidValidator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.re2j.Pattern;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {

  // Not concerned about memory footprint here
  private static final int PREPOPULATION_LOCK_STRIPE_COUNT = 1000;
  private static final String DATA_CLASSIFICATION_MVP_FLAG = "data-classification.mvp";
  private static final boolean DEFAULT_DATA_CLASSIFICATION_FEATURE_FLAG_VALUE = false;

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final RedactionStrategy defaultParamTypeRedactionStrategy;
  private final Striped<Lock> stripedPrepopulationLock =
      Striped.lazyWeakLock(PREPOPULATION_LOCK_STRIPE_COUNT);
  private final boolean defaultAutomaticSecretRedactionEnabled;
  private final InvalidJsonPolicy deafaultInvalidJsonPolicy;
  private final boolean defaultFullPrivacyModeEnabled;
  private final DefaultRedactionRules defaultRedactionRules;
  private final LoadingCache<ContextualKey<Void>, DefaultRedactionRulePopulationStatus>
      prePopulationStatusCache =
          CacheBuilder.newBuilder()
              .maximumSize(10000)
              .build(CacheLoader.from(key -> this.fetchPrepopulationStatus(key.getContext())));
  private final RedactionRuleConfigStore redactionRuleConfigStore;
  private final AutomaticSecretRedactionStrategyConfigStore
      automaticSecretRedactionStrategyConfigStore;
  private final InvalidJsonPolicyConfigStore invalidJsonPolicyConfigStore;
  private final FullPrivacyModeConfigStore fullPrivacyModeConfigStore;
  private final DefaultRedactionRulePopulationStatusStore defaultRedactionRulePopulationStatusStore;
  private final DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceBlockingStub;
  private final FeatureFlagServiceBlockingStub featureFlagServiceBlockingStub;
  private final LoadingCache<ContextualKey<Void>, Boolean> dataClassificationEnabledByTenant;
  private final Duration requestTimeout;

  @Inject
  ConfigServiceCoordinatorImpl(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      SensitiveDataServiceConfig config,
      RedactionRuleConfigStore redactionRuleConfigStore,
      AutomaticSecretRedactionStrategyConfigStore automaticSecretRedactionStrategyConfigStore,
      InvalidJsonPolicyConfigStore invalidJsonPolicyConfigStore,
      FullPrivacyModeConfigStore fullPrivacyModeConfigStore,
      DefaultRedactionRulePopulationStatusStore defaultRedactionRulePopulationStatusStore,
      DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub,
      FeatureFlagServiceBlockingStub featureFlagServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.defaultRedactionRules = config.defaultRedactionRules();
    this.defaultAutomaticSecretRedactionEnabled = config.defaultAutomaticRedactionStrategy();
    this.deafaultInvalidJsonPolicy = config.defaultInvalidJsonPolicy();
    this.defaultFullPrivacyModeEnabled = config.defaultFullPrivacyMode();
    this.defaultParamTypeRedactionStrategy = config.defaultParamTypeRedactionStrategy();
    this.redactionRuleConfigStore = redactionRuleConfigStore;
    this.automaticSecretRedactionStrategyConfigStore = automaticSecretRedactionStrategyConfigStore;
    this.invalidJsonPolicyConfigStore = invalidJsonPolicyConfigStore;
    this.fullPrivacyModeConfigStore = fullPrivacyModeConfigStore;
    this.defaultRedactionRulePopulationStatusStore = defaultRedactionRulePopulationStatusStore;
    this.dataClassificationConfigServiceBlockingStub = dataClassificationConfigServiceBlockingStub;
    this.featureFlagServiceBlockingStub = featureFlagServiceBlockingStub;
    this.requestTimeout = config.getRequestTimeout();
    this.dataClassificationEnabledByTenant =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(config.getRefreshDuration())
            .expireAfterWrite(config.getExpirationDuration())
            .build(
                CacheLoader.asyncReloading(
                    getDataClassificationFeatureFlagCacheLoader(),
                    Executors.newFixedThreadPool(
                        config.getThreadPoolSize(), this.buildThreadFactory())));
  }

  @Override
  public void upsertParamTypeRedactionStrategyConfig(
      RequestContext requestContext,
      ParamType paramType,
      ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig) {
    ParamTypeRedactionStrategyConfigStore.createInstance(
            configServiceBlockingStub, configChangeEventGenerator, paramType)
        .upsertObject(requestContext, paramTypeRedactionStrategyConfig);
  }

  @Override
  public RedactionStrategy getParamTypeRedactionStrategy(
      RequestContext requestContext, ParamType paramType) {
    return ParamTypeRedactionStrategyConfigStore.createInstance(
            configServiceBlockingStub, configChangeEventGenerator, paramType)
        .getData(requestContext, paramType.name())
        .map(ParamTypeRedactionStrategyConfig::getRedactionStrategy)
        .orElse(defaultParamTypeRedactionStrategy);
  }

  @Override
  public void upsertAutomaticSecretRedactionStrategyConfig(
      RequestContext requestContext,
      AutomaticSecretRedactionStrategyConfig automaticSecretRedactionStrategyConfig) {
    this.automaticSecretRedactionStrategyConfigStore.upsertObject(
        requestContext, automaticSecretRedactionStrategyConfig);
  }

  @Override
  public boolean isAutomaticSecretRedactionStrategyEnabled(RequestContext requestContext) {
    return this.automaticSecretRedactionStrategyConfigStore
        .getData(requestContext)
        .map(AutomaticSecretRedactionStrategyConfig::isEnabled)
        .orElse(defaultAutomaticSecretRedactionEnabled);
  }

  @Override
  public void upsertInvalidJsonPolicyConfig(
      RequestContext requestContext, InvalidJsonPolicy invalidJsonPolicy) {
    this.invalidJsonPolicyConfigStore.upsertObject(requestContext, invalidJsonPolicy);
  }

  @Override
  public InvalidJsonPolicy getInvalidJsonPolicy(RequestContext requestContext) {
    return this.invalidJsonPolicyConfigStore
        .getData(requestContext)
        .orElse(deafaultInvalidJsonPolicy);
  }

  @Override
  public RedactionRule createRedactionRule(
      RequestContext requestContext, NewRedactionRule newRedactionRule) {
    return createRedactionRule(requestContext, buildRedactionRuleWithId(newRedactionRule));
  }

  private RedactionRule createRedactionRule(
      RequestContext requestContext, RedactionRule newRedactionRule) {
    validateRegex(newRedactionRule.getRegex());
    return this.redactionRuleConfigStore
        .upsertObject(requestContext, new RedactionRuleConfig(newRedactionRule))
        .getData()
        .getRedactionRule();
  }

  @Override
  public RedactionRule updateRedactionRule(
      RequestContext requestContext, RedactionRule redactionRule) {
    validateRegex(redactionRule.getRegex());
    long creationTimestamp =
        getRedactionRuleConfig(requestContext, redactionRule.getId()).getCreationTimestamp();
    return this.redactionRuleConfigStore
        .upsertObject(requestContext, new RedactionRuleConfig(redactionRule, creationTimestamp))
        .getData()
        .getRedactionRule();
  }

  @Override
  public List<RedactionRule> getRedactionRules(
      RequestContext requestContext, boolean includeConditionalRules) {
    this.insertPrepopulatedRulesIfRequired(requestContext);

    RedactionRuleFilter.Builder filterBuilder = RedactionRuleFilter.newBuilder();
    if (!includeConditionalRules) {
      filterBuilder.setIsConditional(false);
    }

    if (!this.isFullPrivacyModeEnabled(requestContext)) {
      // Unpersisted rules are for full privacy
      filterBuilder.setIsPersisted(true);
    }

    return this.gatherUnfilteredRedactionRules(requestContext)
        .filter(rule -> this.redactionRuleMatchesFilter(rule, filterBuilder.build()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<RedactionRule> getAllRedactionRules(
      RequestContext requestContext, RedactionRuleFilter filter) {
    this.insertPrepopulatedRulesIfRequired(requestContext);
    return this.gatherUnfilteredRedactionRules(requestContext)
        .filter(rule -> this.redactionRuleMatchesFilter(rule, filter))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public RedactionRule deleteRedactionRule(RequestContext requestContext, String redactionRuleId) {
    return this.redactionRuleConfigStore
        .deleteObject(requestContext, redactionRuleId)
        .map(ContextualConfigObject::getData)
        .map(RedactionRuleConfig::getRedactionRule)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  @Override
  public boolean isFullPrivacyModeEnabled(RequestContext requestContext) {
    return this.fullPrivacyModeConfigStore
        .getData(requestContext)
        .map(FullPrivacyModeConfig::getEnabled)
        .orElse(defaultFullPrivacyModeEnabled);
  }

  @Override
  public void upsertFullPrivacyModeConfig(
      RequestContext requestContext, FullPrivacyModeConfig fullPrivacyModeConfig) {
    this.fullPrivacyModeConfigStore.upsertObject(requestContext, fullPrivacyModeConfig);
  }

  private void insertPrepopulatedRulesIfRequired(RequestContext requestContext) {
    Lock prepopulationLock = this.stripedPrepopulationLock.get(requestContext.getTenantId());
    prepopulationLock.lock();
    try {
      DefaultRedactionRulePopulationStatus status = this.getPrepopulationStatus(requestContext);

      if (this.defaultRedactionRules.isPrepopulationComplete(status)) {
        return;
      }

      this.defaultRedactionRules
          .getRulesToPrepopulate(status)
          .forEach(
              (defaultId, rule) -> {
                log.info("Prepopulating rule {} for tenant {}", rule, requestContext.getTenantId());
                RedactionRule insertedRule = this.createRedactionRule(requestContext, rule);
                try {
                  this.updatePrepopulationStatus(
                      requestContext, status.withAdditionalPopulatedRules(Set.of(defaultId)));
                } catch (Exception exception) {
                  log.error(
                      "Failed to update status for prepopulated rule {} for tenant {}, attempting to delete",
                      insertedRule,
                      requestContext.getTenantId(),
                      exception);

                  this.deleteRedactionRule(requestContext, insertedRule.getId());
                  log.error("Delete successful: rule id {} removed", insertedRule.getId());
                }
              });

      DefaultRedactionRulePopulationStatus completedStatus =
          this.defaultRedactionRules.completedPrepopulationStatus(status);

      this.updatePrepopulationStatus(requestContext, completedStatus);

      log.info(
          "Prepopulation complete: {} for tenant: {}",
          completedStatus,
          requestContext.getTenantId());
    } finally {
      prepopulationLock.unlock();
    }
  }

  private List<RedactionRule> getUnpersistedDefaultRules() {
    return this.defaultRedactionRules.getDefaultRules();
  }

  private Stream<RedactionRule> gatherUnfilteredRedactionRules(RequestContext requestContext) {
    return Streams.concat(
        this.getUnpersistedDefaultRules().stream(),
        this.fetchPersistedRules(requestContext).stream());
  }

  private boolean redactionRuleMatchesFilter(RedactionRule rule, RedactionRuleFilter filter) {
    // This can be made prettier if we start expanding to a bunch more conditions

    boolean passesPersistenceFilter =
        !filter.hasIsPersisted() || this.redactionRuleIsPersisted(rule) == filter.getIsPersisted();

    boolean passesConditionalFilter =
        !filter.hasIsConditional()
            || this.redactionRuleIsConditional(rule) == filter.getIsConditional();

    boolean passesSensitiveFilter =
        !filter.hasIsSensitive() || this.redactionRuleIsSensitive(rule) == filter.getIsSensitive();

    return passesPersistenceFilter && passesConditionalFilter && passesSensitiveFilter;
  }

  private boolean redactionRuleIsConditional(RedactionRule rule) {
    return !rule.getConditionsList().isEmpty();
  }

  private boolean redactionRuleIsPersisted(RedactionRule rule) {
    // Assuming a valid uuid indicates a persisted rule
    return UuidValidator.isValid(rule.getId());
  }

  private boolean redactionRuleIsSensitive(RedactionRule rule) {
    // Currently only core mode rules do not mark the data as sensitive
    return !rule.getCategory().equals(CORE_MODE_RULE_CATEGORY);
  }

  private List<RedactionRule> fetchPersistedRules(RequestContext requestContext) {
    return this.redactionRuleConfigStore.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .sorted()
        .map(RedactionRuleConfig::getRedactionRule)
        .collect(Collectors.toUnmodifiableList());
  }

  private void updatePrepopulationStatus(
      RequestContext requestContext, DefaultRedactionRulePopulationStatus updatedStatus) {
    this.defaultRedactionRulePopulationStatusStore.upsertObject(requestContext, updatedStatus);
    this.prePopulationStatusCache.put(requestContext.buildContextualKey(), updatedStatus);
  }

  private DefaultRedactionRulePopulationStatus getPrepopulationStatus(
      RequestContext requestContext) {
    // Prepopulation's write status is only dependent on tenant
    ContextualKey<Void> key =
        RequestContext.forTenantId(requestContext.getTenantId().orElseThrow()).buildContextualKey();
    // If populated with a complete value use it, else invalidate and refetch
    return Optional.ofNullable(this.prePopulationStatusCache.getIfPresent(key))
        .filter(this.defaultRedactionRules::isPrepopulationComplete)
        .orElseGet(
            () -> {
              this.prePopulationStatusCache.invalidate(key);
              return this.prePopulationStatusCache.getUnchecked(key);
            });
  }

  private DefaultRedactionRulePopulationStatus fetchPrepopulationStatus(
      RequestContext requestContext) {
    return this.defaultRedactionRulePopulationStatusStore
        .getData(requestContext)
        .orElseGet(
            () ->
                DefaultRedactionRulePopulationStatus.empty()
                    .forAutomaticRedactionState(
                        () -> this.isAutomaticSecretRedactionStrategyEnabled(requestContext)));
  }

  private RedactionRule buildRedactionRuleWithId(NewRedactionRule newRedactionRule) {
    RedactionRule.Builder builder =
        RedactionRule.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setName(newRedactionRule.getName())
            .setDescription(newRedactionRule.getDescription())
            .setCategory(newRedactionRule.getCategory())
            .setRedactionStrategy(newRedactionRule.getRedactionStrategy())
            .setMatchType(newRedactionRule.getMatchType())
            .setRegex(newRedactionRule.getRegex())
            .setSessionIdentifier(newRedactionRule.getSessionIdentifier())
            .addAllConditions(newRedactionRule.getConditionsList())
            .setFqn(newRedactionRule.getFqn());
    if (newRedactionRule.hasComplexData()) {
      builder.setComplexData(newRedactionRule.getComplexData());
    }
    return builder.build();
  }

  private RedactionRuleConfig getRedactionRuleConfig(
      RequestContext requestContext, String redactionRuleId) {
    return redactionRuleConfigStore
        .getData(requestContext, redactionRuleId)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private void validateRegex(String redactionRuleRegex) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(redactionRuleRegex);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex", e);
    }
  }

  public List<DataType> getAllDataTypes(RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub.getDataTypes(
                    GetDataTypesRequest.getDefaultInstance()))
        .getDataTypesList()
        .stream()
        .filter(this::isNotLegacyDataType)
        .collect(Collectors.toUnmodifiableList());
  }

  public List<DataSet> getAllDataSets(RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                dataClassificationConfigServiceBlockingStub.getDataSets(
                    GetDataSetsRequest.getDefaultInstance()))
        .getDataSetsList()
        .stream()
        .filter(dataSet -> dataSet.getInfo().getEnabled())
        .collect(Collectors.toUnmodifiableList());
  }

  public boolean isDataClassificationEnabled(RequestContext requestContext) {
    ContextualKey<Void> contextualKey = requestContext.buildContextualKey();
    try {
      return this.dataClassificationEnabledByTenant.getUnchecked(contextualKey);
    } catch (Exception e) {
      log.error(
          "Error fetching data classification feature flag from cache for tenant {}",
          requestContext.getTenantId(),
          e);
    }
    return DEFAULT_DATA_CLASSIFICATION_FEATURE_FLAG_VALUE;
  }

  private CacheLoader<ContextualKey<Void>, Boolean> getDataClassificationFeatureFlagCacheLoader() {
    return new CacheLoader<>() {
      @Override
      public Boolean load(ContextualKey<Void> contextualKey) {
        Map<String, FeatureFlagValue> featureFlagValueMap =
            contextualKey
                .getContext()
                .call(
                    () ->
                        featureFlagServiceBlockingStub
                            .withDeadlineAfter(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)
                            .getCurrentFlagValues(
                                GetCurrentFlagValuesRequest.newBuilder()
                                    .addFlagKeys(DATA_CLASSIFICATION_MVP_FLAG)
                                    .build()))
                .getValuesMap();
        FeatureFlagValue featureFlagValue =
            Optional.ofNullable(featureFlagValueMap.get(DATA_CLASSIFICATION_MVP_FLAG))
                .orElseThrow(IllegalStateException::new);
        return featureFlagValue.getBoolean();
      }
    };
  }

  private boolean isNotLegacyDataType(DataType dataType) {
    return !dataType.getRule().getScopedPatternsList().isEmpty();
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("feature-flag-cache-%d")
        .build();
  }
}
