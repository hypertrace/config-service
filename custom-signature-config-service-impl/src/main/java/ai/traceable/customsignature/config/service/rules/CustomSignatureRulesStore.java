package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.audit.utils.AuditContextualObjectUtils.contextualObjectWithDefaultTraceableAuditInfo;
import static ai.traceable.audit.utils.AuditContextualObjectUtils.enrichWithDefaultAuditInfo;
import static ai.traceable.audit.utils.AuditDetailsBuilder.buildAuditDetails;
import static ai.traceable.audit.utils.AuditFilterUtils.matchesAuditFilters;
import static ai.traceable.customsignature.config.service.CustomSignatureConstants.CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.customsignature.config.service.CustomSignatureConstants.CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME;
import static java.util.Objects.nonNull;

import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CountryRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureRulesStore
    extends IdentifiedObjectStoreWithFilter<CustomSignatureRule, GetRulesFilter> {

  private final CustomSignatureRuleConverter customSignatureRuleConverter;
  private final Map<String, ContextualConfigObject<CustomSignatureRule>>
      defaultCustomSignatureRuleObjectsMap;
  private final CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig;

  private static final Set<ContextualKey<Void>> PROCESSED_RULE_TENANT_IDS = new HashSet<>();

  @Inject
  public CustomSignatureRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      CustomSignatureRuleConverter customSignatureRuleConverter,
      ConfigChangeEventGenerator configChangeEventGenerator,
      CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig) {
    super(
        configServiceBlockingStub,
        CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE,
        CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.customSignatureRuleConverter = customSignatureRuleConverter;
    this.defaultCustomSignatureRuleObjectsMap =
        buildDefaultRuleObjectsMap(
            customSignatureConfigServiceConfig.getDefaultCustomSignatureRules());
    this.customSignatureConfigServiceConfig = customSignatureConfigServiceConfig;
  }

  @Override
  public Optional<ContextualConfigObject<CustomSignatureRule>> getObject(
      RequestContext context, String id) {
    return super.getObject(context, id)
        .map(
            contextualObj ->
                enrichWithDefaultAuditInfo(
                    contextualObj, defaultCustomSignatureRuleObjectsMap.get(id)))
        .or(() -> Optional.ofNullable(defaultCustomSignatureRuleObjectsMap.get(id)));
  }

  @Override
  public List<ContextualConfigObject<CustomSignatureRule>> getAllObjects(RequestContext context) {
    List<ContextualConfigObject<CustomSignatureRule>> persistedRules = super.getAllObjects(context);

    // Start with defaults, then overlay persisted rules (enriching with default audit info if
    // needed)
    Map<String, ContextualConfigObject<CustomSignatureRule>> mergedRulesMap =
        new HashMap<>(defaultCustomSignatureRuleObjectsMap);
    persistedRules.forEach(
        rule ->
            mergedRulesMap.put(
                rule.getData().getId(),
                enrichWithDefaultAuditInfo(
                    rule, defaultCustomSignatureRuleObjectsMap.get(rule.getData().getId()))));

    return List.copyOf(mergedRulesMap.values());
  }

  @Override
  public List<ContextualConfigObject<CustomSignatureRule>> getAllObjects(
      RequestContext context, GetRulesFilter filter) {
    return getAllObjects(context).stream()
        .filter(configObject -> filterConfigData(configObject.getData(), filter).isPresent())
        .filter(
            configObject ->
                matchesAuditFilters(
                    configObject,
                    filter.getAuditFilter(),
                    customSignatureConfigServiceConfig.getUserVisibleEmailConfig()))
        .collect(Collectors.toUnmodifiableList());
  }

  public List<CustomSignatureRuleRecord> getAllRuleRecords(RequestContext context) {
    return getBackwardCompatibleExistingRuleRecords(context, null);
  }

  public List<CustomSignatureRuleRecord> getAllRuleRecords(
      RequestContext context, GetRulesFilter filter) {
    return getBackwardCompatibleExistingRuleRecords(context, filter);
  }

  @Override
  protected Optional<CustomSignatureRule> buildDataFromValue(Value value) {
    try {
      return Optional.of(customSignatureRuleConverter.convert(value));
    } catch (InvalidProtocolBufferException exception) {
      log.error("Unable to convert config to custom signature rule for rule: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(CustomSignatureRule data) {
    return customSignatureRuleConverter.convert(data);
  }

  @Override
  protected String getContextFromData(CustomSignatureRule data) {
    return data.getId();
  }

  private Map<String, ContextualConfigObject<CustomSignatureRule>> buildDefaultRuleObjectsMap(
      List<CustomSignatureRule> defaultRules) {
    return defaultRules.stream()
        .map(rule -> contextualObjectWithDefaultTraceableAuditInfo(rule, rule.getId()))
        .collect(Collectors.toMap(ContextualConfigObject::getContext, Function.identity()));
  }

  @Override
  protected Optional<CustomSignatureRule> filterConfigData(
      CustomSignatureRule rule, GetRulesFilter filter) {
    return Optional.of(rule)
        .filter(
            filteredRule ->
                filter.getRuleIdsCount() == 0
                    || filter.getRuleIdsList().contains(filteredRule.getId()))
        .filter(
            filteredRule ->
                filter.getEventTypesList().isEmpty()
                    || filter.getEventTypesList().contains(filteredRule.getEffect().getEventType()))
        .filter(
            filteredRule ->
                !(filter.hasDisabled() && filteredRule.getDisabled() != filter.getDisabled()))
        .filter(
            filteredRule ->
                !(filter.hasInternal() && filteredRule.getInternal() != filter.getInternal()))
        .filter(filteredRule -> filterOnCustomSecRulePresent(filteredRule, filter))
        .filter(filteredRule -> filterRuleOnScope(filteredRule, filter.getRuleScope()))
        .filter(filteredRule -> filterRuleOnSource(filteredRule, filter.getRuleSourcesList()))
        .filter(filteredRule -> filterRuleOnLabels(filteredRule, filter.getLabelKeysList()))
        .filter(
            filteredRule ->
                filterRuleOnRuleEvaluationPoints(
                    filteredRule, filter.getRuleEvaluationPointsList()))
        .filter(filteredRule -> filterRuleOnCategories(filteredRule, filter.getCategoriesList()))
        .filter(filteredRule -> filterRuleOnLabels(filteredRule, filter.getLabelsMap()));
  }

  private boolean filterRuleOnCategories(
      CustomSignatureRule rule, List<Category> categoriesInFilter) {
    return categoriesInFilter.isEmpty() || categoriesInFilter.contains(rule.getCategory());
  }

  private boolean filterRuleOnLabels(CustomSignatureRule rule, Map<String, String> labelsInFilter) {
    if (labelsInFilter.isEmpty()) {
      return true;
    }
    Map<String, String> ruleLabels = rule.getDefinition().getLabelsMap();
    return labelsInFilter.entrySet().stream()
        .anyMatch(
            entry -> {
              String key = entry.getKey();
              String value = entry.getValue();
              return ruleLabels.containsKey(key)
                  && (StringUtils.isEmpty(value) || ruleLabels.get(key).equals(value));
            });
  }

  private boolean filterRuleOnSource(
      CustomSignatureRule rule, List<RuleSource> ruleSourcesInFilter) {
    return ruleSourcesInFilter.isEmpty() || ruleSourcesInFilter.contains(rule.getRuleSource());
  }

  private boolean filterRuleOnLabels(CustomSignatureRule rule, List<String> labelKeysInFilter) {
    return labelKeysInFilter.isEmpty()
        || rule.getDefinition().getLabelsMap().keySet().containsAll(labelKeysInFilter);
  }

  private boolean filterRuleOnRuleEvaluationPoints(
      CustomSignatureRule rule, List<RuleEvaluationPoint> ruleEvaluationPointsInFilter) {
    return ruleEvaluationPointsInFilter.isEmpty()
        || rule.getEffect().getRuleEvaluationPointsList().stream()
            .anyMatch(ruleEvaluationPointsInFilter::contains);
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(CustomSignatureRule ruleData, RuleScope filterScope) {
    List<String> ruleEnvironmentIds =
        ruleData.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  private boolean filterOnCustomSecRulePresent(CustomSignatureRule rule, GetRulesFilter filter) {
    if (!filter.hasContainsSecRuleClause()) {
      return true;
    }
    boolean hasCustomSecRule =
        rule.getDefinition().getClauseGroup().getClausesList().stream()
            .anyMatch(Clause::hasCustomSecRule);
    return (hasCustomSecRule && filter.getContainsSecRuleClause())
        || (!hasCustomSecRule && !filter.getContainsSecRuleClause());
  }

  private List<CustomSignatureRuleRecord> getBackwardCompatibleExistingRuleRecords(
      RequestContext context, @Nullable GetRulesFilter filter) {
    List<CustomSignatureRuleRecord> existingRecords = fetchExistingRecords(context, filter);

    List<CustomSignatureRule> existingRules = extractRules(existingRecords);
    List<CustomSignatureRule> backwardCompatibleRules =
        getCustomSignatureRules(
            new RequestContext(context).withUserTrackingSuppressed(), existingRules);

    Map<String, CustomSignatureRule> backwardCompatibleRulesById =
        indexRulesById(backwardCompatibleRules);

    return makeRecordsBackwardCompatible(existingRecords, backwardCompatibleRulesById);
  }

  private List<CustomSignatureRuleRecord> fetchExistingRecords(
      RequestContext context, @Nullable GetRulesFilter filter) {
    return (nonNull(filter) ? getAllObjects(context, filter) : getAllObjects(context))
        .stream().map(this::toRuleRecord).collect(Collectors.toList());
  }

  private List<CustomSignatureRule> extractRules(List<CustomSignatureRuleRecord> records) {
    return records.stream().map(CustomSignatureRuleRecord::getRule).collect(Collectors.toList());
  }

  private Map<String, CustomSignatureRule> indexRulesById(List<CustomSignatureRule> rules) {
    return rules.stream()
        .collect(Collectors.toMap(CustomSignatureRule::getId, Function.identity()));
  }

  private List<CustomSignatureRuleRecord> makeRecordsBackwardCompatible(
      List<CustomSignatureRuleRecord> records,
      Map<String, CustomSignatureRule> backwardCompatibleRulesById) {
    return records.stream()
        .map(
            ruleRecord -> {
              String ruleId = ruleRecord.getRule().getId();
              CustomSignatureRule backwardCompatibleRule = backwardCompatibleRulesById.get(ruleId);
              return backwardCompatibleRule != null
                  ? rebuildRecordWithRule(ruleRecord, backwardCompatibleRule)
                  : null;
            })
        .filter(java.util.Objects::nonNull)
        .map(this::mergeRegionFieldsBackwardCompatible)
        .collect(Collectors.toList());
  }

  private CustomSignatureRuleRecord rebuildRecordWithRule(
      CustomSignatureRuleRecord originalRecord, CustomSignatureRule updatedRule) {
    return CustomSignatureRuleRecord.newBuilder()
        .setRule(updatedRule)
        .setAuditDetails(originalRecord.getAuditDetails())
        .build();
  }

  private CustomSignatureRuleRecord toRuleRecord(
      ContextualConfigObject<CustomSignatureRule> contextual) {
    return CustomSignatureRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(
            buildAuditDetails(
                contextual, customSignatureConfigServiceConfig.getUserVisibleEmailConfig()))
        .build();
  }

  // processing existing custom signature rule, when attribute key value expression does not have
  // key condition.
  private List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, List<CustomSignatureRule> existingCustomSignatureRule) {
    if (!PROCESSED_RULE_TENANT_IDS.contains(requestContext.buildInternalContextualKey())) {
      List<CustomSignatureRule> customSignatureRules = new ArrayList<>();
      List<CustomSignatureRule> processedCustomSignatureRules = new ArrayList<>();
      for (CustomSignatureRule rule : existingCustomSignatureRule) {
        Optional<CustomSignatureRule> updateCustomSignatureRule =
            updateCustomSignatureRuleIfApplicable(rule);
        if (updateCustomSignatureRule.isEmpty()) {
          customSignatureRules.add(rule);
        } else {
          processedCustomSignatureRules.add(updateCustomSignatureRule.get());
        }
      }
      if (!processedCustomSignatureRules.isEmpty()) {
        customSignatureRules.addAll(
            super.upsertObjects(requestContext, processedCustomSignatureRules).stream()
                .map(ConfigObject::getData)
                .collect(Collectors.toUnmodifiableList()));
      }
      PROCESSED_RULE_TENANT_IDS.add(requestContext.buildInternalContextualKey());
      return customSignatureRules;
    }
    return existingCustomSignatureRule;
  }

  // processing Clause with AttributeKeyValueExpression to have key and value condition for backward
  // compatibility.
  private Optional<CustomSignatureRule> updateCustomSignatureRuleIfApplicable(
      CustomSignatureRule rule) {
    boolean anyClauseUpdated = false;
    List<Clause> clauses = new ArrayList<>();
    for (Clause clause : rule.getDefinition().getClauseGroup().getClausesList()) {
      if (clause.hasAttributeKeyValueExpression()
          && !clause.getAttributeKeyValueExpression().hasKeyCondition()) {
        clauses.add(
            clause.toBuilder()
                .setAttributeKeyValueExpression(processAttributeKeyValueExpression(clause))
                .build());
        anyClauseUpdated = true;
      } else {
        clauses.add(clause);
      }
    }
    return anyClauseUpdated ? Optional.of(processRuleDefinition(rule, clauses)) : Optional.empty();
  }

  private AttributeKeyValueExpression processAttributeKeyValueExpression(Clause clause) {
    AttributeKeyValueExpression.Builder attributeKeyValueExpressionBuilder =
        clause.getAttributeKeyValueExpression().toBuilder();
    AttributeKeyValueExpression.Builder builder =
        attributeKeyValueExpressionBuilder.setKeyCondition(
            getStringCondition(
                attributeKeyValueExpressionBuilder.getMatchKey(),
                attributeKeyValueExpressionBuilder.getKeyMatchOperator()));
    if (nonNull(attributeKeyValueExpressionBuilder.getMatchValue())
        && !attributeKeyValueExpressionBuilder.getMatchValue().isBlank()
        && !attributeKeyValueExpressionBuilder
            .getValueMatchOperator()
            .equals(MatchOperator.MATCH_OPERATOR_UNSPECIFIED)) {
      builder.setValueCondition(
          getStringCondition(
              attributeKeyValueExpressionBuilder.getMatchValue(),
              attributeKeyValueExpressionBuilder.getValueMatchOperator()));
    }
    return builder.build();
  }

  private CustomSignatureRule processRuleDefinition(
      CustomSignatureRule rule, List<Clause> updateClauses) {
    ClauseGroup clauseGroup =
        rule.getDefinition().getClauseGroup().toBuilder()
            .clearClauses()
            .addAllClauses(updateClauses)
            .build();

    RuleDefinition ruleDefinition =
        rule.getDefinition().toBuilder().setClauseGroup(clauseGroup).build();

    return rule.toBuilder().setDefinition(ruleDefinition).build();
  }

  private StringCondition getStringCondition(String value, MatchOperator operator) {
    return StringCondition.newBuilder().setValue(value).setOperator(operator).build();
  }

  private CustomSignatureRuleRecord mergeRegionFieldsBackwardCompatible(
      CustomSignatureRuleRecord ruleRecord) {
    CustomSignatureRule rule = ruleRecord.getRule();
    if (!rule.hasDefinition()) {
      return ruleRecord;
    }
    ClauseGroup expanded = expandClauseGroupRegions(rule.getDefinition().getClauseGroup());
    CustomSignatureRule expandedRule =
        rule.toBuilder()
            .setDefinition(rule.getDefinition().toBuilder().setClauseGroup(expanded))
            .build();
    return ruleRecord.toBuilder().setRule(expandedRule).build();
  }

  private ClauseGroup expandClauseGroupRegions(ClauseGroup clauseGroup) {
    return ClauseGroup.newBuilder()
        .setClauseOperator(clauseGroup.getClauseOperator())
        .addAllClauses(
            clauseGroup.getClausesList().stream()
                .map(this::expandClauseRegion)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private Clause expandClauseRegion(Clause clause) {
    if (clause.hasClauseGroup()) {
      return clause.toBuilder()
          .setClauseGroup(expandClauseGroupRegions(clause.getClauseGroup()))
          .build();
    }
    if (clause.getClauseCase() != Clause.ClauseCase.REGION_EXPRESSION) {
      return clause;
    }
    RegionExpression regionExpression = clause.getRegionExpression();
    RegionExpression.Builder regionBuilder = regionExpression.toBuilder();

    Set<String> existingNewIsoCodes =
        regionExpression.getRegionsList().stream()
            .filter(RegionIdentifier::hasCountry)
            .map(RegionIdentifier::getCountry)
            .map(CountryRegionIdentifier::getIsoCode)
            .collect(Collectors.toUnmodifiableSet());

    regionExpression.getRegionIdentifiersList().stream()
        .map(RegionExpression.Region::getCountryIsoCode)
        .filter(iso -> !iso.isEmpty() && !existingNewIsoCodes.contains(iso))
        .map(
            iso ->
                RegionIdentifier.newBuilder()
                    .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode(iso))
                    .build())
        .forEach(regionBuilder::addRegions);

    regionBuilder.clearRegionIdentifiers();
    for (RegionIdentifier regionIdentifier : regionBuilder.getRegionsList()) {
      if (regionIdentifier.hasCountry()) {
        regionBuilder.addRegionIdentifiers(
            RegionExpression.Region.newBuilder()
                .setCountryIsoCode(regionIdentifier.getCountry().getIsoCode()));
      }
    }

    return clause.toBuilder().setRegionExpression(regionBuilder).build();
  }
}
