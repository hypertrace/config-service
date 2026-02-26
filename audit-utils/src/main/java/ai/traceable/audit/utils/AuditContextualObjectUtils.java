package ai.traceable.audit.utils;

import static ai.traceable.audit.utils.AuditFilterUtils.isNullOrPlaceHolderEmail;

import java.time.Instant;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;

@Slf4j
@UtilityClass
public class AuditContextualObjectUtils {

  public static final String TRACEABLE = "Traceable";

  public <T> ContextualConfigObject<T> contextualObjectWithDefaultTraceableAuditInfo(
      T data, String id) {
    return ContextualObjectImpl.<T>builder()
        .context(id)
        .data(data)
        .createdByEmail(TRACEABLE)
        .lastUserUpdateEmail(TRACEABLE)
        .lastUpdateEmail(TRACEABLE)
        .build();
  }

  public <T> ContextualConfigObject<T> enrichWithDefaultAuditInfo(
      ContextualConfigObject<T> persistedRule, @Nullable ContextualConfigObject<T> defaultRule) {
    if (defaultRule != null) {
      return mergeAuditInfo(persistedRule, defaultRule);
    }
    return persistedRule;
  }

  private <T> ContextualConfigObject<T> mergeAuditInfo(
      ContextualConfigObject<T> persistedRule, ContextualConfigObject<T> defaultRule) {

    String resolvedLastUserUpdateEmail = persistedRule.getLastUserUpdateEmail();
    Instant resolvedLastUserUpdateTimestamp = persistedRule.getLastUserUpdateTimestamp();
    if (isNullOrPlaceHolderEmail(persistedRule.getLastUserUpdateEmail())) {
      resolvedLastUserUpdateEmail = defaultRule.getLastUserUpdateEmail();
      resolvedLastUserUpdateTimestamp = defaultRule.getLastUserUpdateTimestamp();
    }

    return ContextualObjectImpl.<T>builder()
        .context(persistedRule.getContext())
        .data(persistedRule.getData())
        .creationTimestamp(defaultRule.getCreationTimestamp())
        .createdByEmail(defaultRule.getCreatedByEmail())
        .lastUpdatedTimestamp(persistedRule.getLastUpdatedTimestamp())
        .lastUpdateEmail(persistedRule.getLastUpdateEmail())
        .lastUserUpdateTimestamp(resolvedLastUserUpdateTimestamp)
        .lastUserUpdateEmail(resolvedLastUserUpdateEmail)
        .build();
  }
}
