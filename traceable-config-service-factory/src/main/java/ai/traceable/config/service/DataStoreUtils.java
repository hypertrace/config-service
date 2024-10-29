package ai.traceable.config.service;

import ai.traceable.config.service.metric.DBMetricsReporter;
import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.model.config.TypesafeConfigDatastoreConfigExtractor;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class DataStoreUtils {

  private static final String GENERIC_CONFIG_SERVICE_CONFIG = "generic.config.service";
  private static final String DOC_STORE_CONFIG_KEY = "document.store";
  private static final String DATA_STORE_TYPE = "dataStoreType";

  public static Datastore initDataStore(Config config, PlatformServiceLifecycle lifecycle) {
    Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE_CONFIG);
    Config docStoreConfig = genericConfig.getConfig(DOC_STORE_CONFIG_KEY);
    Datastore datastore =
        DatastoreProvider.getDatastore(
            TypesafeConfigDatastoreConfigExtractor.from(docStoreConfig, DATA_STORE_TYPE).extract());
    new DBMetricsReporter(datastore, lifecycle).monitor();
    lifecycle.shutdownComplete().thenRun(datastore::close);
    return datastore;
  }
}
