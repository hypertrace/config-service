package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.customsignature.config.service.CustomSignatureConstants.CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.customsignature.config.service.CustomSignatureConstants.CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureRulesStore
    extends IdentifiedObjectStoreWithFilter<CustomSignatureRule, GetRulesFilter> {

  private final CustomSignatureRuleConverter customSignatureRuleConverter;
  private final List<CustomSignatureRule> defaultCustomSignatureRules;

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
    this.defaultCustomSignatureRules =
        customSignatureConfigServiceConfig.getDefaultCustomSignatureRules();
  }

  @Override
  public List<CustomSignatureRule> getAllConfigData(RequestContext requestContext) {
    return mergeCustomSignatureRules(
        getCustomSignatureRules(requestContext, super.getAllConfigData(requestContext)),
        defaultCustomSignatureRules);
  }

  @Override
  public List<CustomSignatureRule> getAllConfigData(
      RequestContext requestContext, GetRulesFilter filter) {
    List<CustomSignatureRule> filteredDefaultCustomSignatureRules =
        defaultCustomSignatureRules.stream()
            .filter(rule -> filterConfigData(rule, filter).isPresent())
            .collect(Collectors.toUnmodifiableList());
    return mergeCustomSignatureRules(
        getCustomSignatureRules(requestContext, super.getAllConfigData(requestContext, filter)),
        filteredDefaultCustomSignatureRules);
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

  private List<CustomSignatureRule> mergeCustomSignatureRules(
      List<CustomSignatureRule> customSignatureRules,
      List<CustomSignatureRule> defaultCustomSignatureRules) {

    Map<String, CustomSignatureRule> customSignatureRuleMap = new HashMap<>();
    customSignatureRuleMap.putAll(this.getRuleIdToRuleMap(defaultCustomSignatureRules));
    customSignatureRuleMap.putAll(this.getRuleIdToRuleMap(customSignatureRules));

    return customSignatureRuleMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private Map<String, CustomSignatureRule> getRuleIdToRuleMap(
      List<CustomSignatureRule> customSignatureRules) {
    return customSignatureRules.stream()
        .collect(Collectors.toUnmodifiableMap(CustomSignatureRule::getId, Function.identity()));
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
    if (Objects.nonNull(attributeKeyValueExpressionBuilder.getMatchValue())
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
}
