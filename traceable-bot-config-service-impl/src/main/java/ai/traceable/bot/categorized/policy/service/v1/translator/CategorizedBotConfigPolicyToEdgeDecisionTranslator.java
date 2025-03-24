package ai.traceable.bot.categorized.policy.service.v1.translator;

import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_TRACEABLE_CATEGORIZED_BOTS;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfig;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetails;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetailsConfig;
import ai.traceable.bot.categorized.policy.service.v1.BotScope;
import ai.traceable.bot.categorized.policy.service.v1.BotScope.BotClassification;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotAction;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyDetails;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyFilter;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotPolicyActionConfig;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPoliciesRequest;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.bot.categorized.utils.CategorizedBotStringUtil;
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
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.RuleInfoDecoration;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private static final String THREAT_TYPE_TC_BOTS = "TRACEABLE_CATEGORIZED_BOTS";

  private final CategorizedBotConfigPolicyStoreManager categorizedBotConfigPolicyStoreManager;

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
            .flatMap(this::convertToEdgeDecisionRules)
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

  private Stream<EdgeDecisionRule> convertToEdgeDecisionRules(
      final CategorizedBotConfigPolicy categorizedBotConfigPolicy) {
    final CategorizedBotConfigPolicyDetails categorizedBotPolicyDetails =
        categorizedBotConfigPolicy.getCategorizedBotPolicyDetails();
    final List<BotScope> botScopesList = categorizedBotPolicyDetails.getBotScopesList();
    final List<EdgeDecisionRule> edgeDecisionRules = new ArrayList<>();

    for (final BotScope botScope : botScopesList) {
      if (botScope.hasBotList()) {
        botScope.getBotList().getBotIdsList().stream()
            .filter(CATEGORIZED_BOT_CONFIG_MAP::containsKey)
            .map(CATEGORIZED_BOT_CONFIG_MAP::get)
            .map(CategorizedBotConfig::getId)
            .forEach(
                botId -> {
                  final EdgeDecisionRule edgeDecisionRule =
                      getEdgeDecisionRule(categorizedBotConfigPolicy, botId);
                  edgeDecisionRules.add(edgeDecisionRule);
                });
      } else if (botScope.hasBotClassification()) {
        CATEGORIZED_BOT_CONFIG_MAP.entrySet().stream()
            .filter(
                entry -> {
                  final CategorizedBotDetails categorizedBotDetails =
                      entry.getValue().getCategorizedBotDetails();
                  final BotClassification botClassification = botScope.getBotClassification();
                  final boolean categoryMatches =
                      categorizedBotDetails
                          .getBotCategory()
                          .equals(botClassification.getCategory());
                  if (!botClassification.getSubCategoryList().isEmpty()) {
                    final boolean subCategoryMatches =
                        botClassification
                            .getSubCategoryList()
                            .contains(categorizedBotDetails.getBotSubCategory());
                    return categoryMatches && subCategoryMatches;
                  }
                  return categoryMatches;
                })
            .map(Map.Entry::getKey)
            .map(botId -> getEdgeDecisionRule(categorizedBotConfigPolicy, botId))
            .forEach(edgeDecisionRules::add);
      }
    }

    return edgeDecisionRules.stream();
  }

  private EdgeDecisionRule getEdgeDecisionRule(
      final CategorizedBotConfigPolicy categorizedBotConfigPolicy, final String botId) {
    final EdgeDecisionRule.Builder edgeDecisionRuleBuilder = EdgeDecisionRule.newBuilder();
    edgeDecisionRuleBuilder.setId(
        CategorizedBotStringUtil.joinStrings(botId, categorizedBotConfigPolicy.getId()));
    edgeDecisionRuleBuilder.setPolicyKind(PolicyKind.POLICY_KIND_BOT_MITIGATION);
    edgeDecisionRuleBuilder.setPolicyId(categorizedBotConfigPolicy.getId());

    final CategorizedBotDetails categorizedBotDetails =
        CATEGORIZED_BOT_CONFIG_MAP.get(botId).getCategorizedBotDetails();
    final String botName = CategorizedBotStringUtil.getFormattedBotName(categorizedBotDetails);
    final CategorizedBotConfigPolicyDetails categorizedBotPolicyDetails =
        categorizedBotConfigPolicy.getCategorizedBotPolicyDetails();
    // multiple rules may have same rule name, changed from bot_name_policy_id
    edgeDecisionRuleBuilder.setName(categorizedBotPolicyDetails.getName());
    // this should not be required, as we are skipping rule creation for disabled policies, but
    // added for now
    edgeDecisionRuleBuilder.setRuleStatus(
        EdgeDecisionRuleStatus.newBuilder()
            .setDisabled(!categorizedBotPolicyDetails.getEnabled())
            .setInternal(false)
            .build());
    edgeDecisionRuleBuilder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_TRACEABLE_CATEGORIZED_BOTS);

    if (categorizedBotPolicyDetails.hasCategorizedBotPolicyScope()
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
        buildRuleInfoDecorations(categorizedBotDetails, botId, categorizedBotConfigPolicy.getId());
    edgeDecisionRuleBuilder.setRuleDecision(
        buildEdgeRuleDecision(
            categorizedBotPolicyDetails.getCategorizedBotPolicyActionConfig(),
            ruleInfoDecorations));
    edgeDecisionRuleBuilder.setRuleDefinition(
        buildEdgeDecisionRuleDefinition(botId, botName, categorizedBotConfigPolicy.getId()));
    return edgeDecisionRuleBuilder.build();
  }

  private EdgeDecisionRuleDefinition buildEdgeDecisionRuleDefinition(
      final String botId, final String botName, final String policyId) {
    return EdgeDecisionRuleDefinition.newBuilder()
        .setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST)
        .setCustomFields(Values.of(Structs.of("policyId", Values.of(policyId))))
        .setSignatureRule(
            SignatureRule.newBuilder()
                .setMatchCondition(
                    MatchCondition.newBuilder()
                        .setGenericMatchCondition(
                            GenericMatchCondition.newBuilder()
                                .setJexlExpression(
                                    JexlExpressionConfig.newBuilder()
                                        .setJexlExpression(
                                            String.format(
                                                "%s == true",
                                                CategorizedBotStringUtil
                                                    .getFormattedBotVariableName(botName, botId)))
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private EdgeDecision buildEdgeRuleDecision(
      final CategorizedBotPolicyActionConfig categorizedBotPolicyActionConfig,
      final List<RuleInfoDecoration> ruleInfoDecorations) {
    final EdgeDecision.Builder edgeDecisionBuilder = EdgeDecision.newBuilder();
    edgeDecisionBuilder.setThreatType(THREAT_TYPE_TC_BOTS);
    edgeDecisionBuilder.addAllRuleInfoDecorations(ruleInfoDecorations);
    edgeDecisionBuilder.setEdgeDecisionType(
        translateActionType(categorizedBotPolicyActionConfig.getBotAction()));
    return edgeDecisionBuilder.build();
  }

  private static List<RuleInfoDecoration> buildRuleInfoDecorations(
      final CategorizedBotDetails categorizedBotDetails,
      final String botId,
      final String policyId) {
    return List.of(
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_name").build())
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder().setStringValue(categorizedBotDetails.getName()).build())
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_id").build())
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue(botId).build())
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_category").build())
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder()
                            .setStringValue(categorizedBotDetails.getBotCategory())
                            .build())
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_sub_category").build())
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder()
                            .setStringValue(categorizedBotDetails.getBotSubCategory())
                            .build())
                    .build())
            .build(),
        RuleInfoDecoration.newBuilder()
            .setRuleInfoKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue("bot_policy_id").build())
                    .build())
            .setRuleInfoValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue(policyId).build())
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
