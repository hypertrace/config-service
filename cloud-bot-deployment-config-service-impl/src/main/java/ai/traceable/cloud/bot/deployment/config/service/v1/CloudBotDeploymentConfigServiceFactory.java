package ai.traceable.cloud.bot.deployment.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class CloudBotDeploymentConfigServiceFactory {

  private CloudBotDeploymentConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, Config config, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new CloudBotDeploymentConfigServiceModule(channel, config, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
