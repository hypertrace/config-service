package org.hypertrace.span.processing.config.service.store;

import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleMetadata;

public class IncludeSpanRulesConfigStore extends IdentifiedObjectStore<IncludeSpanRule> {

  private static final String INCLUDE_SPAN_RULES_RESOURCE_NAME = "include-span-rules";
  private static final String INCLUDE_SPAN_RULES_CONFIG_RESOURCE_NAMESPACE =
      "span-processing-rules-config";
  private final TimestampConverter timestampConverter;

  @Inject
  public IncludeSpanRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TimestampConverter timestampConverter) {
    super(
        configServiceBlockingStub,
        INCLUDE_SPAN_RULES_CONFIG_RESOURCE_NAMESPACE,
        INCLUDE_SPAN_RULES_RESOURCE_NAME);
    this.timestampConverter = timestampConverter;
  }

  public List<IncludeSpanRuleDetails> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(
            contextualConfigObject ->
                IncludeSpanRuleDetails.newBuilder()
                    .setRule(contextualConfigObject.getData())
                    .setMetadata(
                        IncludeSpanRuleMetadata.newBuilder()
                            .setCreationTimestamp(
                                timestampConverter.convert(
                                    contextualConfigObject.getCreationTimestamp()))
                            .setLastUpdatedTimestamp(
                                timestampConverter.convert(
                                    contextualConfigObject.getLastUpdatedTimestamp()))
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  @Override
  protected Optional<IncludeSpanRule> buildDataFromValue(Value value) {
    IncludeSpanRule.Builder ruleBuilder = IncludeSpanRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, ruleBuilder);
    return Optional.of(ruleBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(IncludeSpanRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(IncludeSpanRule rule) {
    return rule.getId();
  }
}
