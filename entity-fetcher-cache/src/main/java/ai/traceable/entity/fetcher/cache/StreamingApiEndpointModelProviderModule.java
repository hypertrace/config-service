package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.ApiEndpointModelFetchConfig;
import ai.traceable.platform.insights.models.api.v1.TrainingModelServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class StreamingApiEndpointModelProviderModule extends AbstractModule {

  private static final String INSIGHTS_SERVICE_HOST = "insights.service.config.host";
  private static final String INSIGHTS_SERVICE_PORT = "insights.service.config.port";
  public static final String API_ENDPOINT_MODEL_CONFIG_PATH = "api.endpoint.model.config";

  private final GrpcChannelRegistry grpcChannelRegistry;
  private final Config config;

  public StreamingApiEndpointModelProviderModule(
      GrpcChannelRegistry grpcChannelRegistry, Config config) {
    this.grpcChannelRegistry = grpcChannelRegistry;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(StreamingApiEndpointModelProvider.class)
        .to(DefaultStreamingApiEndpointModelProvider.class);
  }

  @Provides
  ApiEndpointModelFetchConfig providesApiEndpointModelFetchConfig() {
    return ApiEndpointModelFetchConfig.from(
        config.hasPath(API_ENDPOINT_MODEL_CONFIG_PATH)
            ? config.getConfig(API_ENDPOINT_MODEL_CONFIG_PATH)
            : ConfigFactory.empty());
  }

  @Provides
  TrainingModelServiceGrpc.TrainingModelServiceBlockingStub providesTrainingModelServiceStub() {
    return TrainingModelServiceGrpc.newBlockingStub(
            grpcChannelRegistry.forPlaintextAddress(
                config.getString(INSIGHTS_SERVICE_HOST), config.getInt(INSIGHTS_SERVICE_PORT)))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
