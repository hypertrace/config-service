package ai.traceable.jwt.extraction.config.service;

import ai.traceable.jwt.extraction.config.service.impl.DefaultJwtExtractionRule.DeletedDefaultJwtExtractionRule;
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

class DeletedDefaultJwtExtractionRuleStore
    extends IdentifiedObjectStore<DeletedDefaultJwtExtractionRule> {
  private static final String JWT_EXTRACTION_RULE_RESOURCE_NAME = "jwt-extraction-rule";
  private static final String DELETED_DEFAULT_JWT_DETECTION_RULE_NAMESPACE = "deleted-default-rule";

  @Inject
  DeletedDefaultJwtExtractionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DELETED_DEFAULT_JWT_DETECTION_RULE_NAMESPACE,
        JWT_EXTRACTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DeletedDefaultJwtExtractionRule> buildDataFromValue(Value ruleValue) {
    try {
      DeletedDefaultJwtExtractionRule.Builder builder =
          DeletedDefaultJwtExtractionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DeletedDefaultJwtExtractionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(DeletedDefaultJwtExtractionRule rule) {
    return rule.getId();
  }

  void markDefaultIdDeleted(RequestContext requestContext, String id) {
    upsertObject(requestContext, DeletedDefaultJwtExtractionRule.newBuilder().setId(id).build());
  }

  Set<String> getDeletedDefaultIds(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getContext)
        .collect(Collectors.toUnmodifiableSet());
  }
}
