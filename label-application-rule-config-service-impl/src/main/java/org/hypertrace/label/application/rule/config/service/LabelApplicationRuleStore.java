package org.hypertrace.label.application.rule.config.service;

import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;

public class LabelApplicationRuleStore extends IdentifiedObjectStore<LabelApplicationRule> {
  private static final String LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME = "label-config";
  private static final String LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE =
      "label-application-rules";

  LabelApplicationRuleStore(ConfigServiceGrpc.ConfigServiceBlockingStub stub) {
    super(
        stub,
        LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE,
        LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<LabelApplicationRule> buildObjectFromValue(Value value) {
    try {
      LabelApplicationRule.Builder builder = LabelApplicationRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromObject(LabelApplicationRule object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromObject(LabelApplicationRule object) {
    return object.getId();
  }
}
