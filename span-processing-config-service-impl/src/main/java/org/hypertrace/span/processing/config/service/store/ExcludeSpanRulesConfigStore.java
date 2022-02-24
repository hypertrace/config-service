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
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleMetadata;

public class ExcludeSpanRulesConfigStore extends IdentifiedObjectStore<ExcludeSpanRule> {

  private static final String EXCLUDE_SPAN_RULES_RESOURCE_NAME = "exclude-span-rules";
  private static final String EXCLUDE_SPAN_RULES_CONFIG_RESOURCE_NAMESPACE =
      "exclude-span-rules-config";

  @Inject
  public ExcludeSpanRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        EXCLUDE_SPAN_RULES_CONFIG_RESOURCE_NAMESPACE,
        EXCLUDE_SPAN_RULES_RESOURCE_NAME);
  }

  public List<ExcludeSpanRuleDetails> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(
            contextualConfigObject ->
                ExcludeSpanRuleDetails.newBuilder()
                    .setRule(contextualConfigObject.getData())
                    .setMetadata(
                        ExcludeSpanRuleMetadata.newBuilder()
                            .setCreationTimestamp(
                                TimestampConverter.convert(
                                    contextualConfigObject.getCreationTimestamp()))
                            .setLastUpdatedTimestamp(
                                TimestampConverter.convert(
                                    contextualConfigObject.getLastUpdatedTimestamp()))
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  @Override
  protected Optional<ExcludeSpanRule> buildDataFromValue(Value value) {
    ExcludeSpanRule.Builder ruleBuilder = ExcludeSpanRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, ruleBuilder);
    return Optional.of(ruleBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ExcludeSpanRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(ExcludeSpanRule rule) {
    return rule.getId();
  }
}
