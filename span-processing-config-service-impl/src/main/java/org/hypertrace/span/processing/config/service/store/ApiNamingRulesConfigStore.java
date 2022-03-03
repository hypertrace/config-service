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
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleMetadata;

public class ApiNamingRulesConfigStore extends IdentifiedObjectStore<ApiNamingRule> {

  private static final String API_NAMING_RULES_RESOURCE_NAME = "api-naming-rules";
  private static final String API_NAMING_RULES_CONFIG_RESOURCE_NAMESPACE =
      "span-processing-rules-config";
  private final TimestampConverter timestampConverter;

  @Inject
  public ApiNamingRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TimestampConverter timestampConverter) {
    super(
        configServiceBlockingStub,
        API_NAMING_RULES_CONFIG_RESOURCE_NAMESPACE,
        API_NAMING_RULES_RESOURCE_NAME);
    this.timestampConverter = timestampConverter;
  }

  public List<ApiNamingRuleDetails> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(
            contextualConfigObject ->
                ApiNamingRuleDetails.newBuilder()
                    .setRule(contextualConfigObject.getData())
                    .setMetadata(
                        ApiNamingRuleMetadata.newBuilder()
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
  protected Optional<ApiNamingRule> buildDataFromValue(Value value) {
    ApiNamingRule.Builder ruleBuilder = ApiNamingRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, ruleBuilder);
    return Optional.of(ruleBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ApiNamingRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(ApiNamingRule rule) {
    return rule.getId();
  }
}
