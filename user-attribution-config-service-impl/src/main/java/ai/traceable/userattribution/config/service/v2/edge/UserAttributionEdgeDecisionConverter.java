package ai.traceable.userattribution.config.service.v2.edge;

import static ai.traceable.userattribution.config.service.v2.edge.EdgeDecisionJexlStringUtils.escapeSingleQuotes;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.ObfuscationStrategy;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class UserAttributionEdgeDecisionConverter {
  private static final String END_USER_ID_ATTRIBUTE_KEY = "enduser.id";
  private static final String END_USER_ROLE_ATTRIBUTE_KEY = "enduser.role";
  private static final String END_USER_SCOPE_ATTRIBUTE_KEY = "enduser.scope";
  private static final String AUTH_TYPES_ATTRIBUTE_KEY = "traceableai.auth.types";

  private static final DerivationRule IP_ADDRESS_DERIVATION_RULE =
      DerivationRule.newBuilder()
          .setTransformationConfig(
              DataTransformationConfig.newBuilder()
                  .setJexlExpression(
                      JexlExpressionConfig.newBuilder().setJexlExpression("$s.getIpAddress()"))
                  .setOutputType(FieldType.FIELD_TYPE_STR))
          .build();

  @Inject
  public UserAttributionEdgeDecisionConverter() {}

  public EdgeDecisionEngineConfig convert(List<UserAttributionRule> userAttributionRules) {
    List<DerivationRule> userIdDerivationRules =
        userAttributionRules.stream()
            .sorted(
                Comparator.comparingInt(UserAttributionRule::getRank)
                    .thenComparing(UserAttributionRule::getId))
            .map(UserAttributionRule::getData)
            .filter(UserAttributionRuleData::hasUserIdRule)
            .map(ruleData -> convertTokenRule(ruleData, ruleData.getUserIdRule()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(
                Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    VariableDerivationMapping userAttributionVariable =
        VariableDerivationMapping.newBuilder()
            .setName(END_USER_ID_ATTRIBUTE_KEY)
            .addAllRules(userIdDerivationRules)
            .addRules(IP_ADDRESS_DERIVATION_RULE)
            .build();

    List<DerivationRule> userRoleDerivationRules =
        userAttributionRules.stream()
            .sorted(
                Comparator.comparingInt(UserAttributionRule::getRank)
                    .thenComparing(UserAttributionRule::getId))
            .map(UserAttributionRule::getData)
            .filter(UserAttributionRuleData::hasUserRoleRule)
            .map(ruleData -> convertTokenRule(ruleData, ruleData.getUserRoleRule()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(
                Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    VariableDerivationMapping userRoleVariable =
        VariableDerivationMapping.newBuilder()
            .setName(END_USER_ROLE_ATTRIBUTE_KEY)
            .addAllRules(userRoleDerivationRules)
            .build();

    List<DerivationRule> userScopeDerivationRules =
        userAttributionRules.stream()
            .sorted(
                Comparator.comparingInt(UserAttributionRule::getRank)
                    .thenComparing(UserAttributionRule::getId))
            .map(UserAttributionRule::getData)
            .filter(UserAttributionRuleData::hasUserScopeRule)
            .map(ruleData -> convertTokenRule(ruleData, ruleData.getUserScopeRule()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(
                Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    VariableDerivationMapping userScopeVariable =
        VariableDerivationMapping.newBuilder()
            .setName(END_USER_SCOPE_ATTRIBUTE_KEY)
            .addAllRules(userScopeDerivationRules)
            .build();

    List<DerivationRule> authTypesDerivationRules =
        userAttributionRules.stream()
            .sorted(
                Comparator.comparingInt(UserAttributionRule::getRank)
                    .thenComparing(UserAttributionRule::getId))
            .map(UserAttributionRule::getData)
            .filter(UserAttributionRuleData::hasAuthTypeRule)
            .map(ruleData -> convertTokenRule(ruleData, ruleData.getAuthTypeRule()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(
                Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    VariableDerivationMapping authTypesVariable =
        VariableDerivationMapping.newBuilder()
            .setName(AUTH_TYPES_ATTRIBUTE_KEY)
            .addAllRules(authTypesDerivationRules)
            .build();

    return EdgeDecisionEngineConfig.newBuilder()
        .addCommonVariables(userAttributionVariable)
        .addCommonVariables(userRoleVariable)
        .addCommonVariables(userScopeVariable)
        .addCommonVariables(authTypesVariable)
        .build();
  }

  private Optional<DerivationRule> convertTokenRule(
      UserAttributionRuleData ruleData, UserAttributionTokenRule tokenRule) {
    Optional<String> jexlExpressionOptional = generateJexlExpression(ruleData, tokenRule);
    if (jexlExpressionOptional.isEmpty()) {
      return Optional.empty();
    }

    DerivationRule.Builder builder =
        DerivationRule.newBuilder()
            .setTransformationConfig(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression(jexlExpressionOptional.get()))
                    .setOutputType(FieldType.FIELD_TYPE_STR));

    generateMatchCondition(ruleData, tokenRule).ifPresent(builder::setMatchCondition);
    return Optional.of(builder.build());
  }

  private Optional<String> generateJexlExpression(
      UserAttributionRuleData ruleData, UserAttributionTokenRule tokenRule) {
    String base = "$s";
    UserAttributionRootTokenRule root = ruleData.getRootTokenRule();

    if (tokenRule.hasRootRelativeProjection()) {
      if (!root.hasAttributeProjection()) {
        throw new IllegalArgumentException(
            "Root relative projection requires root attribute projection");
      }
      String rootExpr = applyAttributeProjection(base, root.getAttributeProjection());
      String projected =
          applyValueProjections(
              rootExpr, tokenRule.getRootRelativeProjection().getValueProjectionsList());
      return Optional.of(applyObfuscation(tokenRule, projected));
    }

    String leafExpr;
    switch (tokenRule.getProjectionCase()) {
      case ATTRIBUTE_PROJECTION:
        leafExpr = applyAttributeProjection(base, tokenRule.getAttributeProjection());
        leafExpr =
            applyValueProjections(
                leafExpr, tokenRule.getAttributeProjection().getValueProjectionsList());
        break;
      case CUSTOM_PROJECTION:
        // Custom projection is a JSON projector used by agent-attribute config.
        // The edge-decision/JEXL path here does not have a compatible way to execute it.
        // Skip this rule and rely on the IP fallback.
        return Optional.empty();
      case LITERAL_VALUE_PROJECTION:
        leafExpr = tokenRule.getLiteralValueProjection().getLiteralValue().getStringValue();
        leafExpr = String.format("'%s'", escapeSingleQuotes(leafExpr));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported token rule projection: " + tokenRule.getProjectionCase());
    }
    return Optional.of(applyObfuscation(tokenRule, leafExpr));
  }

  private String applyObfuscation(UserAttributionTokenRule tokenRule, String inputJexl) {
    if (tokenRule.getTokenObfuscationStrategy() == ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH) {
      return String.format("traceableTransformUtils:sha256(%s)", inputJexl);
    }
    return inputJexl;
  }

  private Optional<MatchCondition> generateMatchCondition(
      UserAttributionRuleData ruleData, UserAttributionTokenRule tokenRule) {
    try {
      UserAttributionRuleScope scope = ruleData.getScope();
      List<MatchCondition> conditions = new ArrayList<>();
      if (scope.hasEnvironmentScope()) {
        conditions.add(
            UserAttributionMatchConditionConverter.convertEnvironment(scope.getEnvironmentScope()));
      }
      if (scope.hasServiceScope()) {
        throw new IllegalArgumentException("service scope is not supported");
      }
      if (scope.hasUrlScope()) {
        conditions.add(UserAttributionMatchConditionConverter.convertUrl(scope.getUrlScope()));
      }

      UserAttributionRootTokenRule rootTokenRule = ruleData.getRootTokenRule();
      if (tokenRule.hasRootRelativeProjection() && rootTokenRule.hasTokenConditionalPredicate()) {
        conditions.add(
            UserAttributionMatchConditionConverter.convertPredicate(
                rootTokenRule.getTokenConditionalPredicate()));
      }
      if (tokenRule.hasTokenConditionalPredicate()) {
        conditions.add(
            UserAttributionMatchConditionConverter.convertPredicate(
                tokenRule.getTokenConditionalPredicate()));
      }
      if (conditions.isEmpty()) {
        return Optional.empty();
      }
      if (conditions.size() == 1) {
        return Optional.of(conditions.get(0));
      }
      return Optional.of(UserAttributionMatchConditionConverter.andAll(conditions));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private String applyAttributeProjection(String base, AttributeProjection projection) {
    return UserAttributionJexlProjectionUtils.applyAttributeProjection(base, projection);
  }

  private String applyValueProjections(String inputJexl, List<ValueProjection> projections) {
    return UserAttributionJexlProjectionUtils.applyValueProjections(inputJexl, projections);
  }
}
