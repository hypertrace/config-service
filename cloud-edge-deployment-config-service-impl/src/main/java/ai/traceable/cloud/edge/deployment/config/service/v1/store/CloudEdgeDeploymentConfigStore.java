package ai.traceable.cloud.edge.deployment.config.service.v1.store;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CloudEdgeDeploymentConfigStore
    extends IdentifiedObjectStore<CloudEdgeDeploymentConfig> {

  private static final String CLOUD_EDGE_DEPLOYMENT_CONFIG_NAMESPACE = "cloudEdgeDeploymentConfig";
  private static final String CLOUD_EDGE_DEPLOYMENT_CONFIG_RESOURCE_NAME = "cloud-edge-deployment";

  @Inject
  public CloudEdgeDeploymentConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        CLOUD_EDGE_DEPLOYMENT_CONFIG_NAMESPACE,
        CLOUD_EDGE_DEPLOYMENT_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<CloudEdgeDeploymentConfig> buildDataFromValue(Value value) {
    try {
      CloudEdgeDeploymentConfig.Builder builder = CloudEdgeDeploymentConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into CloudEdgeDeploymentConfig failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(CloudEdgeDeploymentConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }

  @Override
  protected String getContextFromData(CloudEdgeDeploymentConfig config) {
    return config.getId();
  }

  public CloudEdgeDeploymentConfig createCloudEdgeDeploymentConfig(
      RequestContext ctx, CloudEdgeDeploymentConfig config, ConfigPermission permission) {

    // Here you would handle permission-based storage logic if needed
    return upsertObject(ctx, config).getData();
  }

  public CloudEdgeDeploymentConfig getCloudEdgeDeploymentConfig(RequestContext ctx, String id) {
    return getData(ctx, id).orElse(null);
  }

  public List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, List<String> ids, ConfigAccessType readAccess) {
    if (ids == null || ids.isEmpty()) {
      return getAllConfigData(ctx);
    } else {
      return ids.stream()
          .map(id -> getCloudEdgeDeploymentConfig(ctx, id))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
  }

  public CloudEdgeDeploymentConfig updateCloudEdgeDeploymentConfig(
      RequestContext ctx, CloudEdgeDeploymentConfig config, ConfigPermission permission) {
    // Here you would handle permission-based storage logic if needed
    return upsertObject(ctx, config).getData();
  }

  public void deleteCloudEdgeDeploymentConfig(RequestContext ctx, String id) {
    deleteObject(ctx, id);
  }
}
