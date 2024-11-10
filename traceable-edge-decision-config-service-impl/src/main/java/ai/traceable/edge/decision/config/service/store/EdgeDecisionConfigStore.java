package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class EdgeDecisionConfigStore extends IdentifiedObjectStore<EdgeDecisionEngineConfig> {
  public static final String EDGE_DECISION_CONFIG_NAMESPACE = "edge-decision-config-namespace";
  public static final String EDGE_DECISION_CONFIG_RESOURCE = "edge-decision-config";
  private static final String CONFIG_VERSION_LATEST = "latest";

  @Inject
  public EdgeDecisionConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_DECISION_CONFIG_NAMESPACE,
        EDGE_DECISION_CONFIG_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EdgeDecisionEngineConfig> buildDataFromValue(Value value) {
    EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    return ConfigProtoConverter.convertToValue(edgeDecisionEngineConfig);
  }

  @Override
  protected String getContextFromData(EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    return getConfigId(edgeDecisionEngineConfig.getId(), edgeDecisionEngineConfig.getVersion());
  }

  public static String getConfigId(String id, String version) {
    if (version == null || version.isEmpty()) {
      version = CONFIG_VERSION_LATEST;
    }
    return String.format("%s:%s", id, version);
  }
}
