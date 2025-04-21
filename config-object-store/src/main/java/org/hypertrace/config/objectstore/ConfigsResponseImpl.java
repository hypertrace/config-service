package org.hypertrace.config.objectstore;

import java.util.List;

class ConfigsResponseImpl<T> implements ConfigsResponse<T> {
  private final List<T> contextualConfigObjects;
  private final long totalCount;

  public ConfigsResponseImpl(List<T> contextualConfigObjects, long totalCount) {
    this.contextualConfigObjects = contextualConfigObjects;
    this.totalCount = totalCount;
  }

  @Override
  public List<T> getContextualConfigObjects() {
    return contextualConfigObjects;
  }

  @Override
  public long totalCount() {
    return totalCount;
  }
}
