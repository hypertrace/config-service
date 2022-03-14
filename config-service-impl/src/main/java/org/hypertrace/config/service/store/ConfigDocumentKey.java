package org.hypertrace.config.service.store;

import lombok.Value;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.core.documentstore.Key;

/** Key for the {@link ConfigDocument} (used by {@link DocumentConfigStore}). */
@Value
public class ConfigDocumentKey implements Key {

  private static final String SEPARATOR = ":";
  private static final String LATEST_VERSION = "latest";

  ConfigResourceContext configResourceContext;

  @Override
  public String toString() {
    return String.join(
        SEPARATOR,
        configResourceContext.getConfigResource().getResourceName(),
        configResourceContext.getConfigResource().getResourceNamespace(),
        configResourceContext.getConfigResource().getTenantId(),
        configResourceContext.getContext(),
        LATEST_VERSION);
  }
}
