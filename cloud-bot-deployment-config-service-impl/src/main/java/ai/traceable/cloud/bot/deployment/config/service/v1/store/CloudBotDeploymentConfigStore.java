package ai.traceable.cloud.bot.deployment.config.service.v1.store;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CloudBotDeploymentConfigStore extends IdentifiedObjectStore<CloudBotDeploymentConfig> {

  private static final String CLOUD_BOT_DEPLOYMENT_CONFIG_NAMESPACE = "cloudBotDeploymentConfig";
  private static final String CLOUD_BOT_DEPLOYMENT_CONFIG_RESOURCE_NAME = "cloud-bot-deployment";

  @Inject
  public CloudBotDeploymentConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        CLOUD_BOT_DEPLOYMENT_CONFIG_NAMESPACE,
        CLOUD_BOT_DEPLOYMENT_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<CloudBotDeploymentConfig> buildDataFromValue(Value value) {
    try {
      CloudBotDeploymentConfig.Builder builder = CloudBotDeploymentConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into CloudBotDeploymentConfig failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(CloudBotDeploymentConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }

  @Override
  protected String getContextFromData(CloudBotDeploymentConfig config) {
    return config.getId();
  }

  public CloudBotDeploymentConfig createCloudBotDeploymentConfig(
      RequestContext ctx, CloudBotDeploymentConfig config) {
    return upsertObject(ctx, config).getData();
  }

  public CloudBotDeploymentConfig getCloudBotDeploymentConfig(RequestContext ctx, String id) {
    return getData(ctx, id).orElse(null);
  }

  public List<CloudBotDeploymentConfig> getAllCloudBotDeploymentConfigs(RequestContext ctx) {
    return getAllConfigData(ctx);
  }

  public CloudBotDeploymentConfig updateCloudBotDeploymentConfig(
      RequestContext ctx, CloudBotDeploymentConfig config) {
    return upsertObject(ctx, config).getData();
  }

  public void deleteCloudBotDeploymentConfig(RequestContext ctx, String id) {
    deleteObject(ctx, id);
  }
}
