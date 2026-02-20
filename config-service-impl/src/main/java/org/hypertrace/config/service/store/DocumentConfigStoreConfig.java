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
  private static final String INTERNAL_PLATFORM_EMAIL_KEY = "internal.platform.email";
  private static final String DEFAULT_INTERNAL_PLATFORM_EMAIL =
      ConfigDocument.DEFAULT_LATEST_UPDATED_USER_EMAIL;

  List<Pattern> customerVisibleExcludedEmailPatterns;
  String internalPlatformEmail;

  public DocumentConfigStoreConfig(
      List<Pattern> customerVisibleExcludedEmailPatterns, String internalPlatformEmail) {
    this.customerVisibleExcludedEmailPatterns = customerVisibleExcludedEmailPatterns;
    this.internalPlatformEmail = internalPlatformEmail;
  }

  public static DocumentConfigStoreConfig from(Config config) {
    List<Pattern> patterns = Collections.emptyList();
    String internalPlatformEmail = DEFAULT_INTERNAL_PLATFORM_EMAIL;

    if (config != null && config.hasPath(GENERIC_CONFIG_SERVICE_CONFIG)) {
      Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE_CONFIG);

      if (genericConfig.hasPath(CUSTOMER_VISIBLE_EXCLUDED_EMAIL_PATTERNS)) {
        patterns =
            genericConfig.getStringList(CUSTOMER_VISIBLE_EXCLUDED_EMAIL_PATTERNS).stream()
                .filter(pattern -> pattern != null && !pattern.trim().isEmpty())
                .map(Pattern::compile)
                .collect(Collectors.toUnmodifiableList());
      }

      if (genericConfig.hasPath(INTERNAL_PLATFORM_EMAIL_KEY)) {
        String configured = genericConfig.getString(INTERNAL_PLATFORM_EMAIL_KEY);
        if (configured != null && !configured.trim().isEmpty()) {
          internalPlatformEmail = configured;
        }
      }
    }

    return new DocumentConfigStoreConfig(patterns, internalPlatformEmail);
  }
}
