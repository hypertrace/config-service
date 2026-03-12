package ai.traceable.customsignature.config.service.rules.converter;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.rules.converter.evaluator.CustomSignatureRuleDefinitionConverter;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinitionGroup;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.processing.common.v1.Entity;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.LogicalOperator;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processor.secrules.v1.*;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSignatureConfigContextConverter {
  private final Map<Clause.ClauseCase, CustomSignatureRuleDefinitionConverter>
      ruleDefinitionConverters;

  @Inject
  public CustomSignatureConfigContextConverter(
      Set<CustomSignatureRuleDefinitionConverter> ruleDefinitionConverters) {
    this.ruleDefinitionConverters =
        ruleDefinitionConverters.stream()
            .collect(
                Collectors.toMap(
                    CustomSignatureRuleDefinitionConverter::getClauseCase, converter -> converter));
  }

  public CustomSignatureConfigContext convert(
      List<CustomSignatureRule> customSignatureRules,
      GetCustomSignatureEvaluationConfigContextRequest request) {
    CustomSignatureConfigContext.Builder builder = CustomSignatureConfigContext.newBuilder();

    Map<ScopeContext, List<CustomSignatureRule>> rulesByScope =
        customSignatureRules.stream().collect(Collectors.groupingBy(this::buildScopeContext));

    List<CustomSignatureRulesContext> rulesContexts =
        rulesByScope.entrySet().stream()
            .map(entry -> buildRulesContext(entry.getKey(), entry.getValue(), request))
            .collect(Collectors.toList());

    builder.addAllRuleContexts(rulesContexts);
    return builder.build();
  }

  private ScopeContext buildScopeContext(CustomSignatureRule rule) {
    ScopeContext.Builder builder = ScopeContext.newBuilder();

    RuleScope ruleScope = rule.getRuleScope();
    if (ruleScope.hasEnvironmentScope()) {
      // Convert environment scope to EntityScope with ENVIRONMENT entity type
      EntityScope.Builder entityScopeBuilder = EntityScope.newBuilder();
      entityScopeBuilder.setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT);

      // Convert environment IDs to Entity objects
      for (String environmentId : ruleScope.getEnvironmentScope().getEnvironmentIdsList()) {
        Entity entity =
            Entity.newBuilder()
                .setId(environmentId)
                .setName(environmentId) // Use ID as name for now
                .build();
        entityScopeBuilder.addEntities(entity);
      }

      // Create Scope with EntityScope
      Scope scope = Scope.newBuilder().setEntityScope(entityScopeBuilder.build()).build();

      builder.addScopes(scope);
    }

    return builder.build();
  }

  private CustomSignatureRulesContext buildRulesContext(
      ScopeContext scopeContext,
      List<CustomSignatureRule> rules,
      GetCustomSignatureEvaluationConfigContextRequest request) {
    CustomSignatureRulesContext.Builder builder = CustomSignatureRulesContext.newBuilder();
    builder.setScopeContext(scopeContext);

    List<CustomSignatureRuleConfig> ruleConfigs =
        rules.stream()
            .map(rule -> buildRuleConfig(rule, request))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    builder.addAllRuleConfigs(ruleConfigs);
    return builder.build();
  }

  private Optional<CustomSignatureRuleConfig> buildRuleConfig(
      CustomSignatureRule rule, GetCustomSignatureEvaluationConfigContextRequest request) {
    try {
      CustomSignatureRuleConfig.Builder builder = CustomSignatureRuleConfig.newBuilder();

      builder.setId(rule.getId());
      builder.setName(rule.getName());
      builder.setDescription(rule.getDescription());

      if (rule.hasBlockingExpiryDetails()) {
        builder.setExpiryDetails(convertExpiryDetails(rule.getBlockingExpiryDetails()));
      }

      ClauseGroup clauseGroup = rule.getDefinition().getClauseGroup();

      List<CustomSignatureRuleDefinition> ruleDefinitions = convertRuleDefinitions(clauseGroup);
      if (ruleDefinitions.isEmpty()) {
        log.warn("No convertible rule definitions found for rule: {}", rule.getId());
        return Optional.empty();
      }

      CustomSignatureRuleDefinitionGroup ruleDefinitionGroup =
          CustomSignatureRuleDefinitionGroup.newBuilder()
              .setOperator(convertLogicalOperator(clauseGroup.getClauseOperator()))
              .addAllRuleDefinitions(ruleDefinitions)
              .build();

      builder.setRuleDefinitionGroup(ruleDefinitionGroup);

      if (hasCustomSecRule(clauseGroup)) {
        builder.setProcessorDetails(buildSecRuleProcessorDetails(request));
      }

      return Optional.of(builder.build());

    } catch (Exception e) {
      log.error("Error converting rule config for rule: {}", rule.getId(), e);
      return Optional.empty();
    }
  }

  private List<CustomSignatureRuleDefinition> convertRuleDefinitions(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .map(this::convertClause)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private Optional<CustomSignatureRuleDefinition> convertClause(Clause clause) {
    if (clause.hasClauseGroup()) {
      return convertClauseGroup(clause.getClauseGroup());
    }

    CustomSignatureRuleDefinitionConverter converter =
        ruleDefinitionConverters.get(clause.getClauseCase());
    if (converter == null) {
      log.warn("No converter found for clause case: {}", clause.getClauseCase());
      return Optional.empty();
    }

    try {
      CustomSignatureRuleDefinition ruleDefinition =
          converter.buildCustomSignatureRuleDefinition(clause);
      return Optional.of(ruleDefinition);
    } catch (Exception e) {
      log.error("Error converting clause: {}", clause.getClauseCase(), e);
      return Optional.empty();
    }
  }

  private Optional<CustomSignatureRuleDefinition> convertClauseGroup(ClauseGroup clauseGroup) {
    List<CustomSignatureRuleDefinition> ruleDefinitions =
        clauseGroup.getClausesList().stream()
            .map(this::convertClause)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    if (ruleDefinitions.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        CustomSignatureRuleDefinition.newBuilder()
            .setCustomSignatureRuleDefinitionGroup(
                CustomSignatureRuleDefinitionGroup.newBuilder()
                    .setOperator(convertLogicalOperator(clauseGroup.getClauseOperator()))
                    .addAllRuleDefinitions(ruleDefinitions)
                    .build())
            .build());
  }

  private boolean hasCustomSecRule(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .anyMatch(
            clause ->
                clause.getClauseCase() == Clause.ClauseCase.CUSTOM_SEC_RULE
                    || (clause.hasClauseGroup() && hasCustomSecRule(clause.getClauseGroup())));
  }

  private LogicalOperator convertLogicalOperator(ClauseOperator clauseOperator) {
    switch (clauseOperator) {
      case CLAUSE_OPERATOR_AND:
        return LogicalOperator.LOGICAL_OPERATOR_AND;
      case CLAUSE_OPERATOR_OR:
        return LogicalOperator.LOGICAL_OPERATOR_OR;
      case CLAUSE_OPERATOR_UNSPECIFIED:
      default:
        return LogicalOperator.LOGICAL_OPERATOR_UNSPECIFIED;
    }
  }

  private ai.traceable.protection.engine.config.customsignature.v1.ExpiryDetails
      convertExpiryDetails(ExpiryDetails sourceExpiryDetails) {
    ai.traceable.protection.engine.config.customsignature.v1.ExpiryDetails.Builder builder =
        ai.traceable.protection.engine.config.customsignature.v1.ExpiryDetails.newBuilder();

    if (sourceExpiryDetails.hasExpiryDuration()) {
      builder.setExpiryDuration(sourceExpiryDetails.getExpiryDuration());
    }
    if (sourceExpiryDetails.hasExpiryTimestampMillis()) {
      builder.setExpiryTimestampMillis(sourceExpiryDetails.getExpiryTimestampMillis());
    }

    return builder.build();
  }

  private SecRuleProcessorDetails buildSecRuleProcessorDetails(
      GetCustomSignatureEvaluationConfigContextRequest request) {
    if (request == null) {
      return getSecRuleProcessorDetails(ModsecRuleVersion.MODSEC_RULE_VERSION_V3);
    }

    CustomModsecRuleVersion customModsecRuleVersion = request.getRuleVersion();
    ModsecRuleVersion modsecRuleVersion = convertToModsecRuleVersion(customModsecRuleVersion);

    return getSecRuleProcessorDetails(modsecRuleVersion);
  }

  private ModsecRuleVersion convertToModsecRuleVersion(
      CustomModsecRuleVersion customModsecRuleVersion) {
    switch (customModsecRuleVersion) {
      case CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS:
        return ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
      case CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3:
        return ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3;
      case CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
        return ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE;
      case CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE:
        return ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE;
      case CUSTOM_MODSEC_RULE_VERSION_V3:
      default:
        return ModsecRuleVersion.MODSEC_RULE_VERSION_V3;
    }
  }

  private SecRuleProcessorDetails getSecRuleProcessorDetails(ModsecRuleVersion ruleVersion) {
    switch (ruleVersion) {
      case MODSEC_RULE_VERSION_V3:
      case MODSEC_RULE_VERSION_TEST_V3:
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_BASIC))
            .build();
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS))
            .build();
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType
                            .MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS_DETECTION_ONLY))
            .build();
      case MODSEC_RULE_VERSION_CORAZA_V3:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3:
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_STABLE)
                    .setDirectivesType(CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_BASIC))
            .build();
      case MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3_DETECTION_ONLY_MODE:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_STABLE)
                    .setDirectivesType(
                        CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_DETECTION_ONLY))
            .build();
      default:
        log.error("Unsupported ModsecRuleVersion: {}", ruleVersion.name());
        throw new IllegalArgumentException("Unsupported ModsecRuleVersion: " + ruleVersion.name());
    }
  }
}
