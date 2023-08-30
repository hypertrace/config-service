package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import ai.traceable.span.processing.config.service.impl.v1.RuleType;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigResponse;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleData;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleSet;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.RetentionAction;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
class SpanIngestionRulesManagerImpl implements SpanIngestionRulesManager {
  private static final RetentionAction DEFAULT_RULE_ACTION =
      RetentionAction.newBuilder()
          .setRetain(RetentionAction.RetainAction.getDefaultInstance())
          .build();
  private final SpanIngestionRulesConfigStore ruleStore;
  private final SpanIngestionRequestValidator validator;
  private final SpanIngestionRuleBuilder ruleBuilder;
  private final RankCalculator<PersistedKeyValueRetentionRule, String> rankCalculator;
  private final ObjectDiffer objectDiffer;

  public GetSpanIngestionConfigResponse getRuleSet(
      RequestContext requestContext, GetSpanIngestionConfigRequest request) {
    this.validator.validateOrThrow(requestContext, request);

    List<PersistedKeyValueRetentionRule> rules =
        this.ruleStore.getAllConfigData(requestContext, request).stream()
            .collect(Collectors.toUnmodifiableList());

    List<KeyValueRetentionRule> requestHeaderRules =
        rules.stream()
            .filter(rule -> rule.getRuleType().equals(RuleType.RULE_TYPE_REQUEST_HEADER_RULE))
            .map(this::getConvertedKeyValueRetentionRule)
            .collect(Collectors.toUnmodifiableList());

    List<KeyValueRetentionRule> responseHeaderRules =
        rules.stream()
            .filter(rule -> rule.getRuleType().equals(RuleType.RULE_TYPE_RESPONSE_HEADER_RULE))
            .map(this::getConvertedKeyValueRetentionRule)
            .collect(Collectors.toUnmodifiableList());

    List<KeyValueRetentionRule> attributeRules =
        rules.stream()
            .filter(rule -> rule.getRuleType().equals(RuleType.RULE_TYPE_ATTRIBUTE_RULE))
            .map(this::getConvertedKeyValueRetentionRule)
            .collect(Collectors.toUnmodifiableList());

    return GetSpanIngestionConfigResponse.newBuilder()
        .setRequestHeaderRuleset(
            KeyValueRetentionRuleSet.newBuilder()
                .addAllRules(requestHeaderRules)
                .setDefault(DEFAULT_RULE_ACTION))
        .setResponseHeaderRuleset(
            KeyValueRetentionRuleSet.newBuilder()
                .addAllRules(responseHeaderRules)
                .setDefault(DEFAULT_RULE_ACTION))
        .setAttributeRuleset(
            KeyValueRetentionRuleSet.newBuilder()
                .addAllRules(attributeRules)
                .setDefault(DEFAULT_RULE_ACTION))
        .build();
  }

  public KeyValueRetentionRule createRule(
      RequestContext requestContext, CreateSpanIngestionRuleRequest request) {
    this.validator.validateOrThrow(requestContext, request);

    PersistedKeyValueRetentionRule newRule = this.ruleBuilder.generateNewRuleWithoutRank(request);
    List<PersistedKeyValueRetentionRule> existingRules =
        this.ruleStore.getFilteredRuleList(
            requestContext, request.getStage(), this.ruleTypeFromRuleCase(request));
    List<PersistedKeyValueRetentionRule> mergedAndRankedRules =
        this.rankCalculator.rankAndMergeNewObject(newRule, existingRules);
    this.ruleStore.upsertObjects(
        requestContext,
        this.objectDiffer.getNewOrUpdatedObjects(existingRules, mergedAndRankedRules));

    PersistedKeyValueRetentionRule createdNewRule =
        mergedAndRankedRules.stream()
            .filter(rule -> rule.getId().equals(newRule.getId()))
            .findFirst()
            .orElseThrow();

    return KeyValueRetentionRule.newBuilder()
        .setId(createdNewRule.getId())
        .setData(createdNewRule.getData())
        .build();
  }

  public void deleteRule(RequestContext requestContext, DeleteSpanIngestionRuleRequest request) {
    this.validator.validateOrThrow(requestContext, request);
    PersistedKeyValueRetentionRule ruleToDelete =
        this.ruleStore
            .getData(requestContext, request.getId())
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    this.ruleStore
        .deleteObject(requestContext, request.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    List<PersistedKeyValueRetentionRule> rulesAfterDelete =
        this.ruleStore.getFilteredRuleList(
            requestContext, ruleToDelete.getIngestionStage(), ruleToDelete.getRuleType());
    List<PersistedKeyValueRetentionRule> rerankedRules =
        this.rankCalculator.rankFromOrder(rulesAfterDelete);

    List<PersistedKeyValueRetentionRule> newOrUpdatedObjects =
        this.objectDiffer.getNewOrUpdatedObjects(rulesAfterDelete, rerankedRules);

    if (!newOrUpdatedObjects.isEmpty()) {
      this.ruleStore.upsertObjects(requestContext, newOrUpdatedObjects);
    }
  }

  public KeyValueRetentionRule updateRule(
      RequestContext requestContext, UpdateSpanIngestionRuleRequest request) {
    PersistedKeyValueRetentionRule existingRule =
        this.ruleStore
            .getData(requestContext, request.getId())
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    this.validator.validateOrThrow(requestContext, request);

    PersistedKeyValueRetentionRule updatedRule =
        this.ruleStore
            .upsertObject(
                requestContext,
                this.ruleBuilder.convertedKeyValueRetentionRule(request, existingRule))
            .getData();

    return KeyValueRetentionRule.newBuilder()
        .setId(updatedRule.getId())
        .setData(updatedRule.getData())
        .build();
  }

  public void rankRules(RequestContext requestContext, RankSpanIngestionRuleRequest request) {
    this.validator.validateOrThrow(requestContext, request);
    PersistedKeyValueRetentionRule ruleToUpdate =
        this.ruleStore
            .getData(requestContext, request.getIdToUpdate())
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    List<PersistedKeyValueRetentionRule> existingRules =
        this.ruleStore.getFilteredRuleList(
            requestContext, ruleToUpdate.getIngestionStage(), ruleToUpdate.getRuleType());
    List<PersistedKeyValueRetentionRule> rerankedRuled =
        request.hasPrecedingRuleId()
            ? this.rankCalculator.rerankAfterOtherObject(
                request.getIdToUpdate(), request.getPrecedingRuleId(), existingRules)
            : this.rankCalculator.rerankAsHighestRank(request.getIdToUpdate(), existingRules);

    List<PersistedKeyValueRetentionRule> newOrUpdatedObjects =
        this.objectDiffer.getNewOrUpdatedObjects(existingRules, rerankedRuled);
    if (!newOrUpdatedObjects.isEmpty()) {
      this.ruleStore.upsertObjects(requestContext, newOrUpdatedObjects);
    }
  }

  private KeyValueRetentionRule getConvertedKeyValueRetentionRule(
      PersistedKeyValueRetentionRule rule) {
    KeyValueRetentionRuleData.Builder builder =
        KeyValueRetentionRuleData.newBuilder()
            .setAction(rule.getData().getAction())
            .setKeyMatch(rule.getData().getKeyMatch());
    if (rule.getData().hasExpiration()) {
      builder.setExpiration(rule.getData().getExpiration());
    }
    return KeyValueRetentionRule.newBuilder().setId(rule.getId()).setData(builder.build()).build();
  }

  private RuleType ruleTypeFromRuleCase(CreateSpanIngestionRuleRequest request) {
    switch (request.getRuleCase()) {
      case REQUEST_HEADER_RULE:
        return RuleType.RULE_TYPE_REQUEST_HEADER_RULE;
      case RESPONSE_HEADER_RULE:
        return RuleType.RULE_TYPE_RESPONSE_HEADER_RULE;
      case ATTRIBUTE_RULE:
        return RuleType.RULE_TYPE_ATTRIBUTE_RULE;
      default:
        throw Status.INVALID_ARGUMENT.asRuntimeException();
    }
  }
}
