package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import ai.traceable.span.processing.config.service.impl.v1.RuleType;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import com.google.inject.Inject;
import io.grpc.Status;

class SpanIngestionRuleBuilder {

  private final UuidGenerator uuidGenerator;

  @Inject
  SpanIngestionRuleBuilder(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public PersistedKeyValueRetentionRule generateNewRuleWithoutRank(
      CreateSpanIngestionRuleRequest request) {
    PersistedKeyValueRetentionRule.Builder builder =
        PersistedKeyValueRetentionRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setIngestionStage(request.getStage());
    this.setRuleData(request, builder);
    return builder.build();
  }

  public PersistedKeyValueRetentionRule convertedKeyValueRetentionRule(
      UpdateSpanIngestionRuleRequest request, PersistedKeyValueRetentionRule existingRule) {
    return PersistedKeyValueRetentionRule.newBuilder()
        .setId(request.getId())
        .setIngestionStage(existingRule.getIngestionStage())
        .setData(request.getKeyValueRetentionRuleData())
        .setRuleType(existingRule.getRuleType())
        .setRank(existingRule.getRank())
        .build();
  }

  private void setRuleData(
      CreateSpanIngestionRuleRequest request, PersistedKeyValueRetentionRule.Builder builder) {
    switch (request.getRuleCase()) {
      case REQUEST_HEADER_RULE:
        builder
            .setData(request.getRequestHeaderRule())
            .setRuleType(RuleType.RULE_TYPE_REQUEST_HEADER_RULE);
        return;
      case RESPONSE_HEADER_RULE:
        builder
            .setData(request.getResponseHeaderRule())
            .setRuleType(RuleType.RULE_TYPE_RESPONSE_HEADER_RULE);
        return;
      case ATTRIBUTE_RULE:
        builder.setData(request.getAttributeRule()).setRuleType(RuleType.RULE_TYPE_ATTRIBUTE_RULE);
        return;
      case RULE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid rule received with unrecognized case: %s", request))
            .asRuntimeException();
    }
  }
}
