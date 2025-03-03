package ai.traceable.bot.categorized.policy.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class CategorizedBotConfigPolicyServiceFactory {

  private CategorizedBotConfigPolicyServiceFactory() {}

  public static BindableService build(
      final Channel channel, final ConfigChangeEventGenerator changeEventGenerator) {
    final Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new CategorizedBotConfigPolicyServiceModule(channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
