package org.hypertrace.config.objectstore;

public interface DeletedContextualConfigObject<T> extends DeletedConfigObject<T> {
  String getContext();
}
