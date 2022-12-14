package ai.traceable.ast.scan.profile.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class AstScanProfileConfigServiceFactory {
  public static BindableService build(Channel channel, Config config) {
    Injector injector =
        Guice.createInjector(new AstScanProfileConfigServiceModule(channel, config));
    return injector.getInstance(BindableService.class);
  }
}
