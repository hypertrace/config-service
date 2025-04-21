package org.hypertrace.config.objectstore;

import java.util.List;

public class ConfigsResponseImpl<T> implements ConfigsResponse<T> {
  private final List<ContextualConfigObject<T>> contextualConfigObjects;
  private final long totalCount;

  public ConfigsResponseImpl(
      List<ContextualConfigObject<T>> contextualConfigObjects, long totalCount) {
    this.contextualConfigObjects = contextualConfigObjects;
    this.totalCount = totalCount;
  }

  @Override
  public List<ContextualConfigObject<T>> getContextualConfigObjects() {
    return contextualConfigObjects;
  }

  @Override
  public long totalCount() {
    return totalCount;
  }
}
