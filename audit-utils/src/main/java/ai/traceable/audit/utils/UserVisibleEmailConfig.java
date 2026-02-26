package ai.traceable.audit.utils;

import static ai.traceable.audit.utils.AuditFilterUtils.UNKNOWN_EMAIL;
import static java.util.Objects.nonNull;

import com.typesafe.config.Config;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class UserVisibleEmailConfig {
  private static final String CONFIG_PREFIX = "generic.config.service.customer.visible.";
  private static final String USER_VISIBLE_EMAIL_CONFIG_KEY =
      CONFIG_PREFIX + "excluded.email.patterns";
  private final List<Pattern> userNonVisibleEmailRegexPattern;

  public UserVisibleEmailConfig(Config config) {
    if (config.hasPath(USER_VISIBLE_EMAIL_CONFIG_KEY)) {
      this.userNonVisibleEmailRegexPattern =
          config.getStringList(USER_VISIBLE_EMAIL_CONFIG_KEY).stream()
              .map(Pattern::compile)
              .collect(Collectors.toList());
    } else {
      this.userNonVisibleEmailRegexPattern = List.of();
    }
  }

  public boolean isVisibleEmail(String email) {
    return nonNull(email)
        && userNonVisibleEmailRegexPattern.stream()
            .noneMatch(pattern -> pattern.matcher(email).matches());
  }

  public String maskEmailIfNotVisible(String email) {
    return isVisibleEmail(email) ? email : UNKNOWN_EMAIL;
  }
}
