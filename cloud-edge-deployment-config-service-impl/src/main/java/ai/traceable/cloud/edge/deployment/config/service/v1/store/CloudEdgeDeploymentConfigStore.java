package ai.traceable.cloud.edge.deployment.config.service.v1.store;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.DomainConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.OriginConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.manager.PermissionBasedConfigResolver;
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
  private final PermissionBasedConfigResolver permissionBasedConfigResolver;

  @Inject
  public CloudEdgeDeploymentConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      PermissionBasedConfigResolver permissionBasedConfigResolver) {
    super(
        configServiceBlockingStub,
        CLOUD_EDGE_DEPLOYMENT_CONFIG_NAMESPACE,
        CLOUD_EDGE_DEPLOYMENT_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.permissionBasedConfigResolver = permissionBasedConfigResolver;
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

  public CloudEdgeDeploymentConfig getCloudEdgeDeploymentConfig(RequestContext ctx, String id) {
    return getData(ctx, id).orElse(null);
  }

  public List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType readAccess) {
    return getAllConfigData(ctx).stream()
        .filter(config -> matchesFilter(config, filter))
        .map(
            config ->
                permissionBasedConfigResolver.getResolvedConfigForReadRequest(config, readAccess))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      CloudEdgeDeploymentConfig config, GetCloudEdgeDeploymentConfigsFilter filter) {
    if (filter.getIdsCount() > 0 && !filter.getIdsList().contains(config.getId())) {
      return false;
    }

    if (filter.getCertificateIdsCount() > 0
        && !isCertificateInUse(config, filter.getCertificateIdsList())) {
      return false;
    }

    return true;
  }

  private boolean isCertificateInUse(
      CloudEdgeDeploymentConfig deployment, List<String> certificateIds) {
    CloudEdgeDeploymentInputConfig inputConfig = deployment.getCloudEdgeDeploymentInputConfig();

    // Check all service configs
    for (ServiceConfig serviceConfig : inputConfig.getServiceConfigsList()) {
      // Check domain configs for certificate usage
      for (DomainConfig domainConfig : serviceConfig.getDomainConfigsList()) {
        if (certificateIds.contains(domainConfig.getCertificateId())) {
          return true;
        }
      }

      // Check origin configs for certificate usage
      for (OriginConfig originConfig : serviceConfig.getOriginConfigsList()) {
        if (originConfig.hasCertificateId()
            && certificateIds.contains(originConfig.getCertificateId())) {
          return true;
        }
      }
    }

    return false;
  }

  public CloudEdgeDeploymentConfig upsertCloudEdgeDeploymentConfig(
      RequestContext ctx, CloudEdgeDeploymentConfig config, ConfigPermission permission) {
    // Here you would handle permission-based storage logic if needed
    CloudEdgeDeploymentConfig existingConfig = getData(ctx, config.getId()).orElse(config);
    return permissionBasedConfigResolver.getResolvedConfigForReadRequest(
        upsertObject(
                ctx,
                permissionBasedConfigResolver.getResolvedConfigForWriteRequest(
                    config, existingConfig, permission.getWrite()))
            .getData(),
        permission.getRead());
  }

  public void deleteCloudEdgeDeploymentConfig(RequestContext ctx, String id) {
    deleteObject(ctx, id);
  }

  private List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, List<String> ids) {
    if (ids == null || ids.isEmpty()) {
      return getAllConfigData(ctx);
    } else {
      return ids.stream()
          .map(id -> getCloudEdgeDeploymentConfig(ctx, id))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
  }
}
