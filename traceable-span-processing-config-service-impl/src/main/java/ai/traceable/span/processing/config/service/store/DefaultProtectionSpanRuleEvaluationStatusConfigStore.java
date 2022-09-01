package ai.traceable.span.processing.config.service.store;

import ai.traceable.span.processing.config.service.v1.DefaultProtectionSpanRuleEvaluationStatus;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class DefaultProtectionSpanRuleEvaluationStatusConfigStore
    extends DefaultObjectStore<DefaultProtectionSpanRuleEvaluationStatus> {

  private static final String DEFAULT_PROTECTION_SPAN_RULE_EVALUATION_CONFIG_RESOURCE_NAME =
      "default-protection-span-rule-evaluation";
  private static final String DEFAULT_PROTECTION_SPAN_RULE_EVALUATION_CONFIG_RESOURCE_NAMESPACE =
      "default-protection-span-rule-evaluation-config";

  @Inject
  public DefaultProtectionSpanRuleEvaluationStatusConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        DEFAULT_PROTECTION_SPAN_RULE_EVALUATION_CONFIG_RESOURCE_NAMESPACE,
        DEFAULT_PROTECTION_SPAN_RULE_EVALUATION_CONFIG_RESOURCE_NAME);
  }

  @SneakyThrows
  @Override
  protected Optional<DefaultProtectionSpanRuleEvaluationStatus> buildDataFromValue(Value value) {
    DefaultProtectionSpanRuleEvaluationStatus.Builder statusBuilder =
        DefaultProtectionSpanRuleEvaluationStatus.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, statusBuilder);
    return Optional.of(statusBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DefaultProtectionSpanRuleEvaluationStatus status) {
    return ConfigProtoConverter.convertToValue(status);
  }
}
