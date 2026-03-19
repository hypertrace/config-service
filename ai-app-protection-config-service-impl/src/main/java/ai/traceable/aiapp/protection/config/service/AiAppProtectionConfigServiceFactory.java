package ai.traceable.aiapp.protection.config.service;

import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;

public class AiAppProtectionConfigServiceFactory {

  private AiAppProtectionConfigServiceFactory() {}

  public static BindableService build(Injector parentInjector, Channel channel) {
    Injector injector =
        parentInjector.createChildInjector(new AiAppProtectionConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
