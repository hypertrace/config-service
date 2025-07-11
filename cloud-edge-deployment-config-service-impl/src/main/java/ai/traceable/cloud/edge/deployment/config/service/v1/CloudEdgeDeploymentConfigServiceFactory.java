package ai.traceable.cloud.edge.deployment.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class CloudEdgeDeploymentConfigServiceFactory {

  private CloudEdgeDeploymentConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new CloudEdgeDeploymentConfigServiceModule(channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
