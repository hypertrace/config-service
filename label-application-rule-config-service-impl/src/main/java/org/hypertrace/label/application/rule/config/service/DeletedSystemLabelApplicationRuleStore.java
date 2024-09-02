package org.hypertrace.label.application.rule.config.service;

import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.label.application.rule.config.service.impl.v1.DeletedSystemLabelApplicationRule;

class DeletedSystemLabelApplicationRuleStore
    extends IdentifiedObjectStore<DeletedSystemLabelApplicationRule> {

  private static final String DELETED_SYSTEM_LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME =
      "deleted-system-label-application-rule-config";
  private static final String LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE = "labels";

  DeletedSystemLabelApplicationRuleStore(
      ConfigServiceBlockingStub stub, ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        stub,
        LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE,
        DELETED_SYSTEM_LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DeletedSystemLabelApplicationRule> buildDataFromValue(Value value) {
    try {
      DeletedSystemLabelApplicationRule.Builder builder =
          DeletedSystemLabelApplicationRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DeletedSystemLabelApplicationRule object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DeletedSystemLabelApplicationRule object) {
    return object.getId();
  }
}
