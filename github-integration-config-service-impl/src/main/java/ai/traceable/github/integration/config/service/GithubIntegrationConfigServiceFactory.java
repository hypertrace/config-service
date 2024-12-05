package ai.traceable.github.integration.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class GithubIntegrationConfigServiceFactory {
  private GithubIntegrationConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new GithubIntegrationConfigServiceModule(channel, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
