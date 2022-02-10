package ai.traceable.span.processing.config.service.store;

import ai.traceable.span.processing.config.service.v1.SpanProcessingRule;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SpanProcessingRulesConfigStore extends IdentifiedObjectStore<SpanProcessingRule> {

  private static final String SPAN_PROCESSING_RULES_RESOURCE_NAME = "span-processing-rules";
  private static final String SPAN_PROCESSING_RULES_CONFIG_RESOURCE_NAMESPACE =
      "span-processing-rules-config";

  @Inject
  public SpanProcessingRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        SPAN_PROCESSING_RULES_CONFIG_RESOURCE_NAMESPACE,
        SPAN_PROCESSING_RULES_RESOURCE_NAME);
  }

  public List<SpanProcessingRule> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  @Override
  protected Optional<SpanProcessingRule> buildDataFromValue(Value value) {
    SpanProcessingRule.Builder ruleBuilder = SpanProcessingRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, ruleBuilder);
    return Optional.of(ruleBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SpanProcessingRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(SpanProcessingRule rule) {
    return rule.getId();
  }
}
