package ai.traceable.sensitivedata.config.service;

import static ai.traceable.data.classification.config.service.v1.SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.CORE_MODE_RULE_CATEGORY;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableSet;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeOrdering;
import ai.traceable.sensitivedata.config.service.v1.FullPrivacyModeConfig;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest.RedactionRuleFilter;
import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.github.f4b6a3.uuid.util.UuidValidator;
import com.google.common.collect.Streams;
import com.google.re2j.Pattern;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final RedactionStrategy defaultParamTypeRedactionStrategy;
  private final boolean defaultAutomaticSecretRedactionEnabled;
  private final InvalidJsonPolicy defaultInvalidJsonPolicy;
  private final boolean defaultFullPrivacyModeEnabled;
  private final DefaultRedactionRules defaultRedactionRules;
  private final SensitiveDataServiceConfig config;
  private final RedactionRuleConfigStore redactionRuleConfigStore;
  private final AutomaticSecretRedactionStrategyConfigStore
      automaticSecretRedactionStrategyConfigStore;
  private final InvalidJsonPolicyConfigStore invalidJsonPolicyConfigStore;
  private final FullPrivacyModeConfigStore fullPrivacyModeConfigStore;
  private final DefaultRedactionRulePersistenceStatusStore
      defaultRedactionRulePersistenceStatusStore;
  private final DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceBlockingStub;

  @Inject
  ConfigServiceCoordinatorImpl(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      SensitiveDataServiceConfig config,
      RedactionRuleConfigStore redactionRuleConfigStore,
      AutomaticSecretRedactionStrategyConfigStore automaticSecretRedactionStrategyConfigStore,
      InvalidJsonPolicyConfigStore invalidJsonPolicyConfigStore,
      FullPrivacyModeConfigStore fullPrivacyModeConfigStore,
      DefaultRedactionRulePersistenceStatusStore defaultRedactionRulePersistenceStatusStore,
      DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.defaultRedactionRules = config.defaultRedactionRules();
    this.defaultAutomaticSecretRedactionEnabled = config.defaultAutomaticRedactionStrategy();
    this.defaultInvalidJsonPolicy = config.defaultInvalidJsonPolicy();
    this.defaultFullPrivacyModeEnabled = config.defaultFullPrivacyMode();
    this.defaultParamTypeRedactionStrategy = config.defaultParamTypeRedactionStrategy();
    this.config = config;
    this.redactionRuleConfigStore = redactionRuleConfigStore;
    this.automaticSecretRedactionStrategyConfigStore = automaticSecretRedactionStrategyConfigStore;
    this.invalidJsonPolicyConfigStore = invalidJsonPolicyConfigStore;
    this.fullPrivacyModeConfigStore = fullPrivacyModeConfigStore;
    this.defaultRedactionRulePersistenceStatusStore = defaultRedactionRulePersistenceStatusStore;
    this.dataClassificationConfigServiceBlockingStub = dataClassificationConfigServiceBlockingStub;
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
        .orElse(defaultInvalidJsonPolicy);
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
    DefaultRedactionRulePersistenceStatus priorRulePersistence =
        this.getDefaultRulePersistence(requestContext);
    if (this.defaultRedactionRules.isDefaultRuleId(redactionRule.getId())
        && !priorRulePersistence.hasRuleBeenPersisted(redactionRule.getId())) {
      this.updateDefaultRulePersistenceStatus(
          requestContext,
          priorRulePersistence.withAdditionalPersistedRuleId(redactionRule.getId()));
      // Default timestamp to 0 if persisting an OOTB rule, also give it a unique ID
      RedactionRuleConfig ruleConfig =
          new RedactionRuleConfig(
              redactionRule.toBuilder().setId(UUID.randomUUID().toString()).build(), 0);
      return this.redactionRuleConfigStore
          .upsertObject(requestContext, ruleConfig)
          .getData()
          .getRedactionRule();
    }

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

    RedactionRuleFilter.Builder filterBuilder = RedactionRuleFilter.newBuilder();
    if (!includeConditionalRules) {
      filterBuilder.setIsConditional(false);
    }

    return this.gatherUnfilteredRedactionRules(requestContext)
        .filter(rule -> this.redactionRuleMatchesFilter(rule, filterBuilder.build()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<RedactionRule> getAllRedactionRules(
      RequestContext requestContext, RedactionRuleFilter filter) {
    return this.gatherUnfilteredRedactionRules(requestContext)
        .filter(rule -> this.redactionRuleMatchesFilter(rule, filter))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteRedactionRule(RequestContext requestContext, String redactionRuleId) {
    if (this.defaultRedactionRules.isDefaultRuleId(redactionRuleId)
        && !this.getDefaultRulePersistence(requestContext).hasRuleBeenPersisted(redactionRuleId)) {
      // Just mark as persisted, so we don't return it from config again
      this.defaultRedactionRules
          .getRule(redactionRuleId)
          .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));

      this.updateDefaultRulePersistenceStatus(
          requestContext,
          this.getDefaultRulePersistence(requestContext)
              .withAdditionalPersistedRuleId(redactionRuleId));
      return;
    }

    this.redactionRuleConfigStore
        .deleteObject(requestContext, redactionRuleId)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
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

  @Override
  public DataClassificationRuleState getDataClassificationRuleState(RequestContext requestContext) {
    Map<DataTypeVariant, List<DataType>> enabledDataTypes =
        requestContext
            .call(
                () ->
                    dataClassificationConfigServiceBlockingStub
                        .withDeadlineAfter(
                            config.defaultClientConfig().getTimeout().toMillis(),
                            TimeUnit.MILLISECONDS)
                        .getDataTypes(
                            GetDataTypesRequest.newBuilder()
                                .setSystemDataSetVersion(SYSTEM_DATA_SET_VERSION_RP1)
                                .setResolveInheritedDetails(true)
                                .setFilter(DataTypeFilter.newBuilder().setEnabled(true))
                                .setOrdering(
                                    DataTypeOrdering.DATA_TYPE_ORDERING_EVALUATION_PRIORITY)
                                .build()))
            .getDataTypesList()
            .stream()
            .collect(groupingBy(this::getDataTypeVariant));

    return new DataClassificationRuleState(
        enabledDataTypes.getOrDefault(DataTypeVariant.NON_LEGACY, emptyList()),
        enabledDataTypes.getOrDefault(DataTypeVariant.FROM_REDACTION_RULE, emptyList()).stream()
            .map(DataType::getId)
            .collect(toUnmodifiableSet()),
        enabledDataTypes.containsKey(DataTypeVariant.FROM_SENSITIVE_HEADERS),
        enabledDataTypes.containsKey(DataTypeVariant.FROM_AUTO_REDACTION));
  }

  private List<RedactionRule> getUnpersistedDefaultRules(RequestContext requestContext) {
    return this.defaultRedactionRules.getUnpersistedRules(
        this.getDefaultRulePersistence(requestContext));
  }

  private Stream<RedactionRule> gatherUnfilteredRedactionRules(RequestContext requestContext) {
    return Streams.concat(
        this.getUnpersistedDefaultRules(requestContext).stream(),
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

  private void updateDefaultRulePersistenceStatus(
      RequestContext requestContext, DefaultRedactionRulePersistenceStatus updatedStatus) {
    this.defaultRedactionRulePersistenceStatusStore.upsertObject(requestContext, updatedStatus);
  }

  private DefaultRedactionRulePersistenceStatus getDefaultRulePersistence(
      RequestContext requestContext) {
    return this.defaultRedactionRulePersistenceStatusStore
        .getData(requestContext)
        .orElseGet(DefaultRedactionRulePersistenceStatus::empty);
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
            .setFqn(newRedactionRule.getFqn())
            .setDisabled(false);
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

  private DataTypeVariant getDataTypeVariant(DataType dataType) {
    if (dataType.getId().equals(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID)) {
      return DataTypeVariant.FROM_SENSITIVE_HEADERS;
    }
    if (dataType.getId().equals(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID)) {
      return DataTypeVariant.FROM_AUTO_REDACTION;
    }
    if (dataType.getRule().getScopedPatternsList().isEmpty()) {
      return DataTypeVariant.FROM_REDACTION_RULE;
    } else {
      return DataTypeVariant.NON_LEGACY;
    }
  }

  private enum DataTypeVariant {
    FROM_REDACTION_RULE,
    FROM_AUTO_REDACTION,
    FROM_SENSITIVE_HEADERS,
    NON_LEGACY
  }
}
