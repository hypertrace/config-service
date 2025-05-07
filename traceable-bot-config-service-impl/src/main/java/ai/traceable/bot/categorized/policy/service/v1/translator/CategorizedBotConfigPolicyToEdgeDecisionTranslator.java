package ai.traceable.bot.categorized.policy.service.v1.translator;

import static ai.traceable.bot.categorized.policy.service.v1.EntityType.ENTITY_TYPE_API;
import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_TRACEABLE_CATEGORIZED_BOTS;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfig;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetails;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetailsConfig;
import ai.traceable.bot.categorized.config.service.v1.translator.CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator;
import ai.traceable.bot.categorized.policy.service.v1.BotScope;
import ai.traceable.bot.categorized.policy.service.v1.BotScope.BotClassification;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotAction;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyDetails;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyFilter;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotPolicyActionConfig;
import ai.traceable.bot.categorized.policy.service.v1.EntityScope;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPoliciesRequest;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.UrlRegexScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.RuleInfoDecoration;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class CategorizedBotConfigPolicyToEdgeDecisionTranslator {

  private static final Map<String, CategorizedBotConfig> CATEGORIZED_BOT_CONFIG_MAP =
      CategorizedBotDetailsConfig.INSTANCE.getAllTraceableCategorizedBots().stream()
          .collect(Collectors.toMap(CategorizedBotConfig::getId, Function.identity()));
  private static final String CATEGORIZED_BOT_EDGE_DECISION_CONFIG =
      "CategorizedBotEdgeDecisionConfig";

  private final CategorizedBotConfigPolicyStoreManager categorizedBotConfigPolicyStoreManager;
  private final CachedApiMappingProvider cachedApiMappingProvider;

  public GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse translate(
      final RequestContext requestContext,
      final GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest
          getCategorizedBotConfigPolicyEdgeDecisionRulesRequest) {
    final List<CategorizedBotConfigPolicy> categorizedBotConfigPolicies =
        categorizedBotConfigPolicyStoreManager
            .get(
                requestContext,
                GetCategorizedBotConfigPoliciesRequest.newBuilder()
                    .setCategorizedBotConfigPolicyFilter(
                        CategorizedBotConfigPolicyFilter.newBuilder()
                            .addAllCategorizedBotConfigPolicyIds(
                                getCategorizedBotConfigPolicyEdgeDecisionRulesRequest
                                        .hasCategorizedBotConfigPolicyEdgeDecisionRulesFilter()
                                    ? getCategorizedBotConfigPolicyEdgeDecisionRulesRequest
                                        .getCategorizedBotConfigPolicyEdgeDecisionRulesFilter()
                                        .getCategorizedBotConfigPolicyIdsList()
                                    : Collections.emptyList())
                            .build())
                    .build())
            .getCategorizedBotConfigPoliciesList();
    final List<EdgeDecisionRule> rules =
        categorizedBotConfigPolicies.stream()
            .filter(
                categorizedBotConfigPolicy ->
                    categorizedBotConfigPolicy.getCategorizedBotPolicyDetails().getEnabled())
            .map(
                categorizedBotConfigPolicy ->
                    convertToEdgeDecisionRule(categorizedBotConfigPolicy, requestContext))
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toUnmodifiableList());
    if (!rules.isEmpty()) {
      return GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse.newBuilder()
          .setEdgeDecisionEngineConfig(
              EdgeDecisionEngineConfig.newBuilder()
                  .setName(CATEGORIZED_BOT_EDGE_DECISION_CONFIG)
                  .setId(CATEGORIZED_BOT_EDGE_DECISION_CONFIG)
                  .addAllDecisionRules(rules)
                  .build())
          .build();
    }
    return GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse.getDefaultInstance();
  }

  private EdgeDecisionRule convertToEdgeDecisionRule(
      final CategorizedBotConfigPolicy categorizedBotConfigPolicy,
      final RequestContext requestContext) {
    final CategorizedBotConfigPolicyDetails categorizedBotPolicyDetails =
        categorizedBotConfigPolicy.getCategorizedBotPolicyDetails();
    final List<BotScope> botScopesList = categorizedBotPolicyDetails.getBotScopesList();
    return getEdgeDecisionRule(categorizedBotConfigPolicy, botScopesList, requestContext);
  }

  private EdgeDecisionRule getEdgeDecisionRule(
      final CategorizedBotConfigPolicy categorizedBotConfigPolicy,
      final List<BotScope> botScopes,
      final RequestContext requestContext) {
    final EdgeDecisionRule.Builder edgeDecisionRuleBuilder = EdgeDecisionRule.newBuilder();
    final CategorizedBotConfigPolicyDetails categorizedBotPolicyDetails =
        categorizedBotConfigPolicy.getCategorizedBotPolicyDetails();
    edgeDecisionRuleBuilder.setName(categorizedBotPolicyDetails.getName());
    edgeDecisionRuleBuilder.setId(categorizedBotConfigPolicy.getId());
    edgeDecisionRuleBuilder.setPolicyKind(PolicyKind.POLICY_KIND_BOT_MITIGATION);
    edgeDecisionRuleBuilder.setPolicyId(categorizedBotConfigPolicy.getId());
    edgeDecisionRuleBuilder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_TRACEABLE_CATEGORIZED_BOTS);
    edgeDecisionRuleBuilder.setRuleStatus(
        EdgeDecisionRuleStatus.newBuilder().setDisabled(false).setInternal(false).build());

    final List<String> botIds = getApplicableBotIds(botScopes);
    if (botIds.isEmpty()) {
      return null;
    }

    // set rule scope
    if (categorizedBotPolicyDetails.hasCategorizedBotPolicyTargetScope()) {
      final List<String> urlRegexes =
          getTargetEndpointUrlRegexes(requestContext, categorizedBotPolicyDetails);
      edgeDecisionRuleBuilder.setRuleScope(
          EdgeDecisionRuleScope.newBuilder()
              .addScopeConditions(
                  EdgeDecisionRuleScopeCondition.newBuilder()
                      .setUrlRegexScope(UrlRegexScope.newBuilder().addAllUrlRegexes(urlRegexes))));
    } else if (categorizedBotPolicyDetails.hasCategorizedBotPolicyScope()
        && categorizedBotPolicyDetails.getCategorizedBotPolicyScope().hasEnvironmentScope()
        && !categorizedBotPolicyDetails
            .getCategorizedBotPolicyScope()
            .getEnvironmentScope()
            .getEnvironmentIdsList()
            .isEmpty()) {
      edgeDecisionRuleBuilder.setRuleScope(
          EdgeDecisionRuleScope.newBuilder()
              .addScopeConditions(
                  EdgeDecisionRuleScopeCondition.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder()
                              .addAllEnvironments(
                                  categorizedBotPolicyDetails
                                      .getCategorizedBotPolicyScope()
                                      .getEnvironmentScope()
                                      .getEnvironmentIdsList())
                              .build()))
              .build());
    }

    final List<RuleInfoDecoration> ruleInfoDecorations =
        buildRuleInfoDecorations(categorizedBotConfigPolicy.getId());
    edgeDecisionRuleBuilder.setRuleDecision(
        buildEdgeRuleDecision(
            categorizedBotPolicyDetails.getCategorizedBotPolicyActionConfig(),
            ruleInfoDecorations,
            categorizedBotPolicyDetails.getName()));
    edgeDecisionRuleBuilder.setRuleDefinition(
        buildEdgeDecisionRuleDefinition(botIds, categorizedBotConfigPolicy.getId()));
    return edgeDecisionRuleBuilder.build();
  }

  private List<String> getApplicableBotIds(final List<BotScope> botScopes) {
    final List<String> botIds = new ArrayList<>();
    for (final BotScope botScope : botScopes) {
      if (botScope.hasBotList()) {
        botScope.getBotList().getBotIdsList().stream()
            .filter(CATEGORIZED_BOT_CONFIG_MAP::containsKey)
            .map(CATEGORIZED_BOT_CONFIG_MAP::get)
            .map(CategorizedBotConfig::getId)
            .forEach(botIds::add);
      } else if (botScope.hasBotClassification()) {
        CATEGORIZED_BOT_CONFIG_MAP.entrySet().stream()
            .filter(
                entry -> {
                  final CategorizedBotDetails categorizedBotDetails =
                      entry.getValue().getCategorizedBotDetails();
                  final BotClassification botClassification = botScope.getBotClassification();
                  final boolean categoryMatches =
                      categorizedBotDetails
                          .getBotCategoryId()
                          .equals(botClassification.getBotCategoryId());
                  if (!botClassification.getBotSubCategoryIdList().isEmpty()) {
                    final boolean subCategoryMatches =
                        botClassification
                            .getBotSubCategoryIdList()
                            .contains(categorizedBotDetails.getBotSubCategoryId());
                    return categoryMatches && subCategoryMatches;
                  }
                  return categoryMatches;
                })
            .map(Map.Entry::getKey)
            .forEach(botIds::add);
      }
    }
    return botIds;
  }

  private List<String> getTargetEndpointUrlRegexes(
      final RequestContext requestContext,
      final CategorizedBotConfigPolicyDetails categorizedBotPolicyDetails) {
    final EntityScope entityScope =
        categorizedBotPolicyDetails.getCategorizedBotPolicyTargetScope().getEntityScope();
    if (!entityScope.getEntityType().equals(ENTITY_TYPE_API)) {
      throw new IllegalArgumentException("Unsupported entity type: " + entityScope.getEntityType());
    }
    final Map<String, Optional<ApiIdentifierEntity>> apiIdentifierEntities =
        cachedApiMappingProvider.getApiIdentifierEntities(
            requestContext, Set.copyOf(entityScope.getEntityIdsList()));
    return apiIdentifierEntities.values().stream()
        .flatMap(Optional::stream)
        .map(apiIdentifierEntity -> String.join("|", apiIdentifierEntity.getResolvedUrlPatterns()))
        .collect(Collectors.toUnmodifiableList());
  }

  private EdgeDecisionRuleDefinition buildEdgeDecisionRuleDefinition(
      final List<String> botIds, final String policyId) {
    return EdgeDecisionRuleDefinition.newBuilder()
        .setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST)
        .setCustomFields(Values.of(Structs.of("policyId", Values.of(policyId))))
        .addRuleVariables(
            CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator.INSTANCE.translate(botIds))
        .setSignatureRule(
            SignatureRule.newBuilder()
                .setMatchCondition(
                    MatchCondition.newBuilder()
                        .setGenericMatchCondition(
                            GenericMatchCondition.newBuilder()
                                .setJexlExpression(
                                    JexlExpressionConfig.newBuilder()
                                        .setJexlExpression("tcBot['botId'] != null")
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private EdgeDecision buildEdgeRuleDecision(
      final CategorizedBotPolicyActionConfig categorizedBotPolicyActionConfig,
      final List<RuleInfoDecoration> ruleInfoDecorations,
      final String policyName) {
    final EdgeDecision.Builder edgeDecisionBuilder = EdgeDecision.newBuilder();
    edgeDecisionBuilder.setThreatType(policyName);
    edgeDecisionBuilder.addAllRuleInfoDecorations(ruleInfoDecorations);
    edgeDecisionBuilder.setEdgeDecisionType(
        translateActionType(categorizedBotPolicyActionConfig.getBotAction()));
    return edgeDecisionBuilder.build();
  }

  private static List<RuleInfoDecoration> buildRuleInfoDecorations(final String policyId) {
    return List.of(
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_name").build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression("tcBot['botName']")
                            .build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_id").build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression("tcBot['botId']")
                            .build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_category").build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression("tcBot['botCategory']")
                            .build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("threat_type").build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression("tcBot['botCategory']")
                            .build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_sub_category").build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression("tcBot['botSubCategory']")
                            .build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_policy_id").build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue(policyId).build())
                    .setOutputType(FIELD_TYPE_STR)
                    .build())
            .build());
  }

  private EdgeDecisionType translateActionType(final CategorizedBotAction botAction) {
    switch (botAction) {
      case CATEGORIZED_BOT_ACTION_BLOCK:
        return EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK;
      case CATEGORIZED_BOT_ACTION_MONITOR:
        return EdgeDecisionType.EDGE_DECISION_TYPE_ALERT;
      case CATEGORIZED_BOT_ACTION_ALLOW:
        return EdgeDecisionType.EDGE_DECISION_TYPE_ALLOW;
      case UNRECOGNIZED:
      case CATEGORIZED_BOT_ACTION_UNSPECIFIED:
      default:
        return EdgeDecisionType.EDGE_DECISION_TYPE_UNSPECIFIED;
    }
  }
}
