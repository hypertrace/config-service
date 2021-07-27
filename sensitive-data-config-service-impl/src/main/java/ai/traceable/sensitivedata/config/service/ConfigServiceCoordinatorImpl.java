package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.CORE_MODE_RULE_CATEGORY;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.DEFAULT_RULE_POPULATION_STATUS;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.FULL_PRIVACY_MODE_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.REDACTION_RULES_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest.RedactionRuleFilter;
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
import com.google.protobuf.Value;
import com.google.re2j.Pattern;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {

  // Not concerned about memory footprint here
  private static final int PREPOPULATION_LOCK_STRIPE_COUNT = 1000;

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final RedactionStrategy defaultParamTypeRedactionStrategy;
  private final Striped<Lock> stripedPrepopulationLock =
      Striped.lazyWeakLock(PREPOPULATION_LOCK_STRIPE_COUNT);
  private final boolean defaultAutomaticSecretRedactionEnabled;
  private final boolean defaultFullPrivacyModeEnabled;
  private final DefaultRedactionRules defaultRedactionRules;
  private final LoadingCache<ContextualKey<Void>, DefaultRedactionRulePopulationStatus>
      prePopulationStatusCache =
          CacheBuilder.newBuilder()
              .maximumSize(10000)
              .build(CacheLoader.from(key -> this.fetchPrepopulationStatus(key.getContext())));

  @Inject
  ConfigServiceCoordinatorImpl(
      ConfigServiceBlockingStub configServiceBlockingStub, SensitiveDataServiceConfig config) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.defaultRedactionRules = config.defaultRedactionRules();
    this.defaultAutomaticSecretRedactionEnabled = config.defaultAutomaticRedactionStrategy();
    this.defaultFullPrivacyModeEnabled = config.defaultFullPrivacyMode();
    this.defaultParamTypeRedactionStrategy = config.defaultParamTypeRedactionStrategy();
  }

  @Override
  public void upsertParamTypeRedactionStrategyConfig(
      RequestContext requestContext,
      ParamType paramType,
      ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(paramType.name())
            .setConfig(paramTypeRedactionStrategyConfig.toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
  }

  @Override
  public RedactionStrategy getParamTypeRedactionStrategy(
      RequestContext requestContext, ParamType paramType) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .addContexts(paramType.name())
            .build();
    Optional<ParamTypeRedactionStrategyConfig> paramTypeRedactionStrategyConfig =
        getConfig(requestContext, getConfigRequest)
            .flatMap(ParamTypeRedactionStrategyConfig::fromValue);
    return paramTypeRedactionStrategyConfig.isPresent()
        ? paramTypeRedactionStrategyConfig.get().getRedactionStrategy()
        : defaultParamTypeRedactionStrategy;
  }

  @Override
  public void upsertAutomaticSecretRedactionStrategyConfig(
      RequestContext requestContext,
      AutomaticSecretRedactionStrategyConfig automaticSecretRedactionStrategyConfig) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setConfig(automaticSecretRedactionStrategyConfig.toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
  }

  @Override
  public boolean isAutomaticSecretRedactionStrategyEnabled(RequestContext requestContext) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .build();
    Optional<AutomaticSecretRedactionStrategyConfig> automaticSecretRedactionStrategyConfig =
        getConfig(requestContext, getConfigRequest)
            .flatMap(AutomaticSecretRedactionStrategyConfig::fromValue);
    return automaticSecretRedactionStrategyConfig
        .map(AutomaticSecretRedactionStrategyConfig::isEnabled)
        .orElse(defaultAutomaticSecretRedactionEnabled);
  }

  @Override
  public RedactionRule createRedactionRule(
      RequestContext requestContext, NewRedactionRule newRedactionRule) {
    return createRedactionRule(requestContext, buildRedactionRuleWithId(newRedactionRule));
  }

  private RedactionRule createRedactionRule(
      RequestContext requestContext, RedactionRule newRedactionRule) {
    validateRegex(newRedactionRule.getRegex());
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(newRedactionRule.getId())
            .setConfig(new RedactionRuleConfig(newRedactionRule).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return newRedactionRule;
  }

  @Override
  public RedactionRule updateRedactionRule(
      RequestContext requestContext, RedactionRule redactionRule) {
    validateRegex(redactionRule.getRegex());
    long creationTimestamp =
        getRedactionRuleConfig(requestContext, redactionRule.getId()).getCreationTimestamp();
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(redactionRule.getId())
            .setConfig(new RedactionRuleConfig(redactionRule, creationTimestamp).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return redactionRule;
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
  public void deleteRedactionRule(RequestContext requestContext, String redactionRuleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(redactionRuleId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  @Override
  public boolean isFullPrivacyModeEnabled(RequestContext requestContext) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(FULL_PRIVACY_MODE_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .build();
    Optional<FullPrivacyModeConfig> fullPrivacyModeConfig =
        getConfig(requestContext, getConfigRequest).flatMap(FullPrivacyModeConfig::fromValue);
    return fullPrivacyModeConfig
        .map(FullPrivacyModeConfig::isEnabled)
        .orElse(defaultFullPrivacyModeEnabled);
  }

  @Override
  public void upsertFullPrivacyModeConfig(
      RequestContext requestContext, FullPrivacyModeConfig fullPrivacyModeConfig) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(FULL_PRIVACY_MODE_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setConfig(fullPrivacyModeConfig.toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
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

    // TODO very temporary default logic for backwards compatibility, where we filtered conditional,
    // persisted rules
    boolean passesBackwardsCompatibilityFilter =
        filter.hasIsConditional()
            || !this.redactionRuleIsPersisted(rule)
            || !this.redactionRuleIsConditional(rule);

    return passesPersistenceFilter
        && passesConditionalFilter
        && passesSensitiveFilter
        && passesBackwardsCompatibilityFilter;
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
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .build();

    return this.fetchAllConfigs(requestContext, getAllConfigsRequest).stream()
        .map(ContextSpecificConfig::getConfig)
        .map(RedactionRuleConfig::fromValue)
        .sorted()
        .map(RedactionRuleConfig::getRedactionRule)
        .collect(Collectors.toUnmodifiableList());
  }

  private void updatePrepopulationStatus(
      RequestContext requestContext, DefaultRedactionRulePopulationStatus updatedStatus) {
    this.upsertConfig(
        requestContext,
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setResourceName(DEFAULT_RULE_POPULATION_STATUS)
            .setConfig(updatedStatus.toValue())
            .build());

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
    return this.getConfig(
            requestContext,
            GetConfigRequest.newBuilder()
                .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
                .setResourceName(DEFAULT_RULE_POPULATION_STATUS)
                .build())
        .map(DefaultRedactionRulePopulationStatus::fromValue)
        .orElseGet(
            () ->
                DefaultRedactionRulePopulationStatus.empty()
                    .forAutomaticRedactionState(
                        () -> this.isAutomaticSecretRedactionStrategyEnabled(requestContext)));
  }

  private Value upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
            context.getRequestHeaders(), () -> configServiceBlockingStub.upsertConfig(request))
        .getConfig();
  }

  private Optional<Value> getConfig(RequestContext context, GetConfigRequest request) {
    try {
      return Optional.of(
          context.call(() -> configServiceBlockingStub.getConfig(request)).getConfig());
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private List<ContextSpecificConfig> fetchAllConfigs(
      RequestContext context, GetAllConfigsRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
            context.getRequestHeaders(), () -> configServiceBlockingStub.getAllConfigs(request))
        .getContextSpecificConfigsList();
  }

  private void deleteConfig(RequestContext context, DeleteConfigRequest request) {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        context.getRequestHeaders(), () -> configServiceBlockingStub.deleteConfig(request));
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
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .addContexts(redactionRuleId)
            .build();
    return RedactionRuleConfig.fromValue(getConfig(requestContext, getConfigRequest).orElseThrow());
  }

  private void validateRegex(String redactionRuleRegex) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(redactionRuleRegex);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex", e);
    }
  }
}
