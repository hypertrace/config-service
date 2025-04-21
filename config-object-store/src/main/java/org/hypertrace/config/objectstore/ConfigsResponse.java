package org.hypertrace.config.objectstore;

import java.util.List;

public interface ConfigsResponse<T> {
  List<ContextualConfigObject<T>> getContextualConfigObjects();

  long totalCount();
}
