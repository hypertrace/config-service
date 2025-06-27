package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
public class ModsecBlobValidator {
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);
  private static final ConcurrentHashMap<String, Boolean> modsecBlobCache =
      new ConcurrentHashMap<>();

  private final UuidGenerator uuidGenerator;

  @Inject
  public ModsecBlobValidator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public boolean validate(
      RequestContext requestContext, String modsecRuleBlob, String serviceNames) {
    if (modsecRuleBlob.isBlank()) {
      return true;
    }

    String modsecBlobHash = uuidGenerator.generateId(modsecRuleBlob);
    if (!modsecBlobCache.containsKey(modsecBlobHash)) {
      log.debug(
          "For customer ID: {}, Created modsec blob: [{}] for the exclusion rules for service: [{}]",
          requestContext.getTenantId().orElse(""),
          modsecRuleBlob,
          serviceNames);

      modsecBlobCache.put(
          modsecBlobHash,
          validateModsecBlob(
              modsecRuleBlob, requestContext.getTenantId().orElse(""), serviceNames));
    }
    return modsecBlobCache.get(modsecBlobHash);
  }

  @VisibleForTesting
  boolean validateModsecBlob(String modsecRuleBlob, String tenantName, String serviceName) {
    try {
      boolean isValid = ModsecRuleEngineUtils.modsecValidate(modsecRuleBlob).isOk();
      if (!isValid && LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Detection exclusion modsec rule validation failed for tenant:{}, service:{}.",
            tenantName,
            serviceName);
      } else if (!isValid) {
        log.debug(
            "Detection exclusion modsec rule validation failed for rule: {} for tenant:{} and service:{}.",
            modsecRuleBlob,
            tenantName,
            serviceName);
      }
      return isValid;
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Exception during detection exclusion modsec rule validation for tenant: {}, service: {}.",
            tenantName,
            serviceName,
            e);
      } else {
        log.debug(
            "Exception during detection exclusion modsec rule validation. Rule content: {}, tenant: {}, service: {}.",
            modsecRuleBlob,
            tenantName,
            serviceName,
            e);
      }
      return false;
    }
  }
}
