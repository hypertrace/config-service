package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ExternalDataClassificationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final Config config;

  ExternalDataClassificationConfigServiceModule(Channel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalDataClassificationConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
  }

  @Provides
  SensitiveDataConfigServiceBlockingStub provideSensitiveDataConfigService() {
    return SensitiveDataConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DataClassificationConfigServiceBlockingStub provideDataClassificationConfigService() {
    return DataClassificationConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
