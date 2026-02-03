package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.platform.utils.ip.IpAddressParsingUtils.parseRawIpRange;

import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.v1.BulkUpdateCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule.Builder;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureRulesManager implements RulesManager {

  private final CustomSignatureRulesStore rulesStore;
  private final List<CustomSignatureRule> defaultCustomSignatureRules;

  @Inject
  public CustomSignatureRulesManager(
      CustomSignatureRulesStore rulesStore, CustomSignatureConfigServiceConfig config) {
    this.rulesStore = rulesStore;
    this.defaultCustomSignatureRules = config.getDefaultCustomSignatureRules();
  }

  @Override
  public List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, GetRulesFilter filter) {
    return this.getCustomSignatureRuleRecords(requestContext, filter).stream()
        .map(CustomSignatureRuleRecord::getRule)
        .collect(Collectors.toList());
  }

  @Override
  public List<CustomSignatureRuleRecord> getCustomSignatureRuleRecords(
      RequestContext requestContext, GetRulesFilter filter) {
    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return rulesStore.getAllRuleRecords(requestContext);
    } else {
      return rulesStore.getAllRuleRecords(requestContext, filter);
    }
  }

  @Override
  public Optional<CustomSignatureRule> createCustomSignatureRule(
      RequestContext requestContext, CreateCustomSignatureRuleRequest createRuleRequest) {
    String ruleId = generateRuleId();
    Builder customSignatureRuleBuilder =
        CustomSignatureRule.newBuilder()
            .setId(ruleId)
            .setName(createRuleRequest.getName())
            .setDescription(createRuleRequest.getDescription())
            .setDefinition(processRuleDefinition(createRuleRequest.getDefinition()))
            .setEffect(createRuleRequest.getEffect())
            .setRuleScope(createRuleRequest.getRuleScope())
            .setDisabled(false)
            .setInternal(createRuleRequest.getInternal())
            .setRuleSource(createRuleRequest.getRuleSource())
            .setCategory(createRuleRequest.getCategory());

    if (createRuleRequest.hasBlockingExpiryDetails()) {
      updateExpiryDetails(customSignatureRuleBuilder, createRuleRequest.getBlockingExpiryDetails());
    }

    return upsertConfig(requestContext, customSignatureRuleBuilder.build());
  }

  @Override
  public Optional<CustomSignatureRule> updateCustomSignatureRule(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    String ruleId = customSignatureRule.getId();
    Optional<CustomSignatureRule> originalRule = getCustomSignatureRule(requestContext, ruleId);
    if (originalRule.isEmpty()) {
      return Optional.empty();
    }
    Builder customSignatureRuleBuilder = CustomSignatureRule.newBuilder(customSignatureRule);
    if (customSignatureRule.hasBlockingExpiryDetails()) {
      updateExpiryDetails(
          customSignatureRuleBuilder, customSignatureRule.getBlockingExpiryDetails());
    }
    customSignatureRuleBuilder.setRuleSource(originalRule.get().getRuleSource());
    customSignatureRuleBuilder.setDefinition(
        processRuleDefinition(customSignatureRule.getDefinition()));
    return upsertConfig(requestContext, customSignatureRuleBuilder.build());
  }

  @Override
  public Optional<CustomSignatureRule> deleteCustomSignatureRule(
      RequestContext requestContext, String id) {
    return rulesStore
        .deleteObject(requestContext, id)
        .flatMap(DeletedContextualConfigObject::getDeletedData);
  }

  @Override
  public void bulkDeleteCustomSignatureRules(RequestContext requestContext, List<String> ids) {
    rulesStore.deleteObjects(requestContext, ids);
  }

  @Override
  public void bulkUpdateCustomSignatureRules(
      RequestContext requestContext, BulkUpdateCustomSignatureRulesRequest request) {
    GetRulesFilter filter = GetRulesFilter.newBuilder().addAllRuleIds(request.getIdsList()).build();
    List<CustomSignatureRule> existingRules = rulesStore.getAllConfigData(requestContext, filter);

    List<CustomSignatureRule> updatedRules =
        existingRules.stream()
            .map(rule -> applyBulkUpdates(rule, request))
            .collect(Collectors.toList());

    rulesStore.upsertObjects(requestContext, updatedRules);
  }

  private CustomSignatureRule applyBulkUpdates(
      CustomSignatureRule rule, BulkUpdateCustomSignatureRulesRequest request) {
    return rule.toBuilder().setDisabled(request.getDisabled()).build();
  }

  private Optional<CustomSignatureRule> getCustomSignatureRule(
      RequestContext requestContext, String ruleId) {
    try {
      return defaultCustomSignatureRules.stream()
          .filter(rule -> rule.getId().equals(ruleId))
          .findFirst()
          .or(() -> rulesStore.getData(requestContext, ruleId));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<CustomSignatureRule> upsertConfig(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    try {
      return Optional.of(rulesStore.upsertObject(requestContext, customSignatureRule).getData());
    } catch (Exception exception) {
      log.error("Unable to update custom signature rule {}", customSignatureRule, exception);
      return Optional.empty();
    }
  }

  private void updateExpiryDetails(
      Builder customSignatureRuleBuilder, ExpiryDetails expiryDetails) {
    ExpiryDetails.Builder expiryDetailsBuilder = ExpiryDetails.newBuilder(expiryDetails);
    if (expiryDetails.hasExpiryDuration() && !expiryDetails.hasExpiryTimestampMillis()) {
      expiryDetailsBuilder.setExpiryTimestampMillis(
          System.currentTimeMillis()
              + Duration.parse(expiryDetails.getExpiryDuration()).toMillis());
    } else if (expiryDetails.hasExpiryTimestampMillis() && !expiryDetails.hasExpiryDuration()) {
      expiryDetailsBuilder.setExpiryDuration(
          Duration.ofMillis(expiryDetails.getExpiryTimestampMillis() - System.currentTimeMillis())
              .toString());
    }
    customSignatureRuleBuilder.setBlockingExpiryDetails(expiryDetailsBuilder.build());
  }

  private RuleDefinition processRuleDefinition(RuleDefinition ruleDefinition) {
    RuleDefinition.Builder builder = ruleDefinition.toBuilder();
    builder.setClauseGroup(processClauseGroup(ruleDefinition.getClauseGroup()));
    return builder.build();
  }

  private ClauseGroup processClauseGroup(ClauseGroup clauseGroup) {
    return ClauseGroup.newBuilder()
        .setClauseOperator(clauseGroup.getClauseOperator())
        .addAllClauses(
            clauseGroup.getClausesList().stream()
                .map(this::processClause)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private Clause processClause(Clause clause) {
    if (clause.getIpAddressExpression().getRawInputIpDataList().isEmpty()) {
      return clause;
    }
    return processRawIpAddressesClause(clause);
  }

  private Clause processRawIpAddressesClause(Clause clause) {
    Clause.Builder builder = clause.toBuilder();
    IpAddressExpression ipAddressExpression = clause.getIpAddressExpression();
    List<String> rawIps = ipAddressExpression.getRawInputIpDataList();
    IpAddressParsingUtils.IpParsingResults parsedResults = parseRawIpRange(rawIps);
    IpAddressExpression.Builder ipAddressExpressionBuilder =
        builder.getIpAddressExpressionBuilder();
    ipAddressExpressionBuilder
        .addAllCidrIpRanges(parsedResults.getIpRanges())
        .addAllIpAddresses(parsedResults.getIpAddresses())
        .setExclude(ipAddressExpression.getExclude());
    return builder.build();
  }
}
