package ai.traceable.certificate.management.config.service.v1;

import ai.traceable.certificate.management.config.service.v1.manager.CertificateConfigManager;
import ai.traceable.certificate.management.config.service.v1.manager.CertificateConfigManagerImpl;
import ai.traceable.certificate.management.config.service.v1.validator.CertificateUsageValidator;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigServiceGrpc;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigServiceGrpc.CloudEdgeDeploymentConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class CertificateManagementConfigServiceModule extends AbstractModule {

  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  CertificateManagementConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CertificateManagementConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(CertificateConfigManager.class).to(CertificateConfigManagerImpl.class);
    bind(CertificateUsageValidator.class);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  CloudEdgeDeploymentConfigServiceBlockingStub provideCloudEdgeDeploymentConfigStub() {
    return CloudEdgeDeploymentConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
