package ai.traceable.integration.config.service;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.List;

public class IntegrationConfigServiceFactory {

  static final String SNYK_INTEGRATION_ANNOTATION = "snykIntegration";

  public static List<BindableService> build(Channel channel) {
    Injector injector = Guice.createInjector(new IntegrationConfigServiceModule(channel));

    return ImmutableList.of(getInjectorInstance(injector, SNYK_INTEGRATION_ANNOTATION));
  }

  private static BindableService getInjectorInstance(Injector injector, String annotation) {
    return injector.getInstance(Key.get(BindableService.class, Names.named(annotation)));
  }
}
