package ai.traceable.anomaly.config.service;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import java.util.List;

public class AnomalyConfigServiceFactory {

  static final String ANOMALY_GLOBAL_CONFIG_ANNOTATION = "anomalyGlobalConfig";
  static final String ANOMALY_EXCLUSION_CONFIG_ANNOTATION = "anomalyExclusionConfig";
  static final String ANOMALY_MODSEC_CONFIG_ANNOTATION = "anomalyModsecConfig";

  public static List<BindableService> build(ManagedChannel channel, Config config) {
    Injector injector = Guice.createInjector(new AnomalyConfigServiceModule(channel, config));

    return ImmutableList.of(
        getInjectorInstance(injector, ANOMALY_GLOBAL_CONFIG_ANNOTATION),
        getInjectorInstance(injector, ANOMALY_EXCLUSION_CONFIG_ANNOTATION),
        getInjectorInstance(injector, ANOMALY_MODSEC_CONFIG_ANNOTATION));
  }

  private static <T> BindableService getInjectorInstance(Injector injector, String annotation) {
    return injector.getInstance(Key.get(BindableService.class, Names.named(annotation)));
  }
}
