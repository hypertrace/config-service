package ai.traceable.region.config.service.regions;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class RegionStoreModule extends AbstractModule {
  static final String COUNTRIES_DATA_PATH = "countriesDataPath";
  private final String countriesDataPath;

  public RegionStoreModule(String countriesDataPath) {
    this.countriesDataPath = countriesDataPath;
  }

  @Override
  protected void configure() {
    bind(RegionStore.class).to(RegionStoreImpl.class);
    bind(String.class)
        .annotatedWith(Names.named(COUNTRIES_DATA_PATH))
        .toInstance(countriesDataPath);
  }
}
