package ai.traceable.region.config.service.regions;

public enum RegionType {
  COUNTRY("country");

  private final String name;

  public String getName() {
    return this.name;
  }

  private RegionType(String name) {
    this.name = name;
  }
}
