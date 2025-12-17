package org.hypertrace.config.service.store;

import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class DocumentConfigStoreConfig {
  private static final String GENERIC_CONFIG_SERVICE_CONFIG = "generic.config.service";
  private static final String CUSTOMER_VISIBLE_EXCLUDED_EMAIL_PATTERNS =
      "customer.visible.excluded.email.patterns";

  List<Pattern> customerVisibleExcludedEmailPatterns;

  public DocumentConfigStoreConfig(List<Pattern> customerVisibleExcludedEmailPatterns) {
    this.customerVisibleExcludedEmailPatterns = customerVisibleExcludedEmailPatterns;
  }

  public static DocumentConfigStoreConfig from(Config config) {
    if (config != null
        && config.hasPath(GENERIC_CONFIG_SERVICE_CONFIG)
        && config
            .getConfig(GENERIC_CONFIG_SERVICE_CONFIG)
            .hasPath(CUSTOMER_VISIBLE_EXCLUDED_EMAIL_PATTERNS)) {
      Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE_CONFIG);
      List<Pattern> patterns =
          genericConfig.getStringList(CUSTOMER_VISIBLE_EXCLUDED_EMAIL_PATTERNS).stream()
              .filter(pattern -> pattern != null && !pattern.trim().isEmpty())
              .map(Pattern::compile)
              .collect(Collectors.toUnmodifiableList());
      return new DocumentConfigStoreConfig(patterns);
    }
    return new DocumentConfigStoreConfig(Collections.emptyList());
  }
}
