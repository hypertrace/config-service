package org.hypertrace.config.objectstore;

public interface ContextualConfigObject<T> extends ConfigObject<T> {
  String getContext();
}
