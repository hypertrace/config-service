package ai.traceable.auth.detection.config.service;

import ai.traceable.auth.detection.config.service.impl.DefaultAuthDetectionRule.DeletedDefaultAuthDetectionRule;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DeletedDefaultAuthDetectionRuleStore
    extends IdentifiedObjectStore<DeletedDefaultAuthDetectionRule> {
  private static final String AUTH_DETECTION_RULE_RESOURCE_NAME = "auth-detection-rule";
  private static final String DELETED_DEFAULT_AUTH_RULE_NAMESPACE = "deleted-default-rule";

  @Inject
  DeletedDefaultAuthDetectionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DELETED_DEFAULT_AUTH_RULE_NAMESPACE,
        AUTH_DETECTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DeletedDefaultAuthDetectionRule> buildDataFromValue(Value ruleValue) {
    try {
      DeletedDefaultAuthDetectionRule.Builder builder =
          DeletedDefaultAuthDetectionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DeletedDefaultAuthDetectionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(DeletedDefaultAuthDetectionRule rule) {
    return rule.getId();
  }

  void markDefaultIdDeleted(RequestContext requestContext, String id) {
    upsertObject(requestContext, DeletedDefaultAuthDetectionRule.newBuilder().setId(id).build());
  }

  Set<String> getDeletedDefaultIds(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getContext)
        .collect(Collectors.toUnmodifiableSet());
  }
}
