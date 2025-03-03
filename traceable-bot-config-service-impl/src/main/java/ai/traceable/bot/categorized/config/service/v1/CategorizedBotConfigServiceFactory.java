package ai.traceable.bot.categorized.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.grpc.BindableService;
import io.grpc.Channel;

public class CategorizedBotConfigServiceFactory {

  private CategorizedBotConfigServiceFactory() {}

  public static BindableService build(final Channel channel) {
    final Injector injector =
        Guice.createInjector(Stage.PRODUCTION, new CategorizedBotConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
