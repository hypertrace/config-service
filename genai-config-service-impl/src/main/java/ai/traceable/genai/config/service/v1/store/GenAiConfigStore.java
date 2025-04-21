package ai.traceable.genai.config.service.v1.store;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiScope;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class GenAiConfigStore extends IdentifiedObjectStore<GenAiConfig> {

  private static final String GEN_AI_CONFIG_NAMESPACE = "genAiConfig";
  private static final String GEN_AI_FEATURE_CONFIG_RESOURCE_NAME = "genAiFeatureConfig";

  private final GenAiConfigConverter configConverter;
  private final GenAiConfigIdGenerator configIdGenerator;

  @Inject
  protected GenAiConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GenAiConfigConverter configConverter,
      GenAiConfigIdGenerator configIdGenerator) {
    super(
        configServiceBlockingStub,
        GEN_AI_CONFIG_NAMESPACE,
        GEN_AI_FEATURE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configConverter = configConverter;
    this.configIdGenerator = configIdGenerator;
  }

  @SneakyThrows
  @Override
  protected Optional<GenAiConfig> buildDataFromValue(Value value) {
    return Optional.of(configConverter.convert(value, GenAiConfig.newBuilder()));
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(GenAiConfig data) {
    return configConverter.convert(data);
  }

  @Override
  protected String getContextFromData(GenAiConfig data) {
    return configIdGenerator.generateId(data.getScope());
  }

  public Optional<GenAiConfig> getGenAiConfigFromStore(
      RequestContext requestContext, GenAiScope scope) {
    String id = configIdGenerator.generateId(scope);
    return this.getData(requestContext, id);
  }
}
