package org.hypertrace.config.service;

import com.google.common.base.Strings;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/** A specific context within a configuration resource. */
@Value
public class ConfigResourceContext {
  private static final String DEFAULT_CONTEXT = "DEFAULT-CONTEXT";
  ConfigResource configResource;
  String context;

  public ConfigResourceContext(@Nonnull ConfigResource configResource) {
    this(configResource, null);
  }

  public ConfigResourceContext(@Nonnull ConfigResource configResource, @Nullable String context) {
    this.context = Strings.isNullOrEmpty(context) ? DEFAULT_CONTEXT : context;
    this.configResource = configResource;
  }
}
