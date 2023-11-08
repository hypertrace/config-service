package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.SpanProcessingConfigConstants;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.DataLocation;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.IngestionStage;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SpanIngestionRulesConfigStore
    extends IdentifiedObjectStoreWithFilter<
        PersistedKeyValueRetentionRule, GetSpanIngestionConfigRequest> {
  private static final String SPAN_INGESTION_CONFIG_RESOURCE_NAME = "span-ingestion-config";

  private final RankCalculator<PersistedKeyValueRetentionRule, String> rankCalculator;
  private final Clock clock;
  private final TimestampConverter timestampConverter;

  @Inject
  SpanIngestionRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RankCalculator<PersistedKeyValueRetentionRule, String> rankCalculator,
      Clock clock,
      TimestampConverter timestampConverter) {
    super(
        configServiceBlockingStub,
        SpanProcessingConfigConstants.RESOURCE_NAMESPACE,
        SPAN_INGESTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.rankCalculator = rankCalculator;
    this.clock = clock;
    this.timestampConverter = timestampConverter;
  }

  protected List<PersistedKeyValueRetentionRule> getFilteredRuleList(
      RequestContext requestContext, IngestionStage ruleStage, DataLocation dataLocation) {
    return this.getAllConfigData(requestContext).stream()
        .filter(rule -> rule.getIngestionStage().equals(ruleStage))
        .filter(rule -> rule.getLocation().equals(dataLocation))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected Optional<PersistedKeyValueRetentionRule> buildDataFromValue(Value ruleValue) {
    try {
      PersistedKeyValueRetentionRule.Builder builder = PersistedKeyValueRetentionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(PersistedKeyValueRetentionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected Optional<PersistedKeyValueRetentionRule> filterConfigData(
      PersistedKeyValueRetentionRule data, GetSpanIngestionConfigRequest request) {
    if (data.getIngestionStage().equals(request.getStage())
        && expiredFilterCriteria(request, data)) {
      return Optional.of(data);
    }
    return Optional.empty();
  }

  private boolean expiredFilterCriteria(
      GetSpanIngestionConfigRequest request, PersistedKeyValueRetentionRule rule) {
    return !request.getFilter().hasIsRuleExpired()
        || request.getFilter().getIsRuleExpired() == isRuleExpired(rule);
  }

  private boolean isRuleExpired(PersistedKeyValueRetentionRule rule) {
    if (!rule.getData().hasExpiration()) {
      return false;
    }
    Instant ruleInstant = timestampConverter.convertToInstant(rule.getData().getExpiration());
    return clock.instant().isAfter(ruleInstant);
  }

  @Override
  protected String getContextFromData(PersistedKeyValueRetentionRule rule) {
    return rule.getId();
  }

  @Override
  protected List<ContextualConfigObject<PersistedKeyValueRetentionRule>> orderFetchedObjects(
      List<ContextualConfigObject<PersistedKeyValueRetentionRule>> objects) {
    return this.rankCalculator.orderFromRanks(objects, ContextualConfigObject::getData);
  }
}
