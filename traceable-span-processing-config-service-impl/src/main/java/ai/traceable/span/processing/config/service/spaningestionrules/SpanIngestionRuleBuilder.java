package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DataLocation;
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
        .setLocation(existingRule.getLocation())
        .setRank(existingRule.getRank())
        .build();
  }

  private void setRuleData(
      CreateSpanIngestionRuleRequest request, PersistedKeyValueRetentionRule.Builder builder) {
    switch (request.getRuleCase()) {
      case REQUEST_HEADER_RULE:
        builder
            .setData(request.getRequestHeaderRule())
            .setLocation(DataLocation.DATA_LOCATION_REQUEST_HEADER);
        return;
      case RESPONSE_HEADER_RULE:
        builder
            .setData(request.getResponseHeaderRule())
            .setLocation(DataLocation.DATA_LOCATION_RESPONSE_HEADER);
        return;
      case ATTRIBUTE_RULE:
        builder
            .setData(request.getAttributeRule())
            .setLocation(DataLocation.DATA_LOCATION_ATTRIBUTE);
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
