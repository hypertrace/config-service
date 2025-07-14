package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
public class ModsecBlobValidator {
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);
  private static final ConcurrentHashMap<String, Boolean> modsecBlobCache =
      new ConcurrentHashMap<>();
  private static final String UNKNOWN_TENANT = "unknown tenant";

  private final UuidGenerator uuidGenerator;

  @Inject
  public ModsecBlobValidator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public boolean validate(
      RequestContext requestContext, String modsecRuleBlob, String serviceName) {
    if (modsecRuleBlob.isBlank()) {
      return true;
    }

    String modsecBlobHash = uuidGenerator.generateId(modsecRuleBlob);
    String tenantID = requestContext.getTenantId().orElse(UNKNOWN_TENANT);
    return modsecBlobCache.computeIfAbsent(
        modsecBlobHash,
        hash -> {
          log.debug(
              "Created modsec blob: [{}] for custom signature rules for the service: [{}] for tenantID: [{}]",
              modsecRuleBlob,
              serviceName,
              tenantID);
          return validateModsecBlob(modsecRuleBlob, tenantID, serviceName);
        });
  }

  @VisibleForTesting
  boolean validateModsecBlob(String modsecRuleBlob, String tenantID, String serviceName) {
    try {
      boolean isValid = ModsecRuleEngineUtils.modsecValidate(modsecRuleBlob).isOk();
      if (!isValid && LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Custom signature modsec rule validation failed for tenantID: {} and serviceName: {}.",
            tenantID,
            serviceName);
      } else if (!isValid) {
        log.debug(
            "Custom signature modsec rule validation failed for modsecRuleBlob: {}, tenantID: {} and serviceName: {}.",
            modsecRuleBlob,
            tenantID,
            serviceName);
      }

      return isValid;
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Exception during custom signature modsec rule validation for tenantID: {} and serviceName: {}.",
            tenantID,
            serviceName,
            e);
      } else {
        log.debug(
            "Exception during custom signature modsec rule validation for modsecRuleBlob: {}, tenantID: {}, serviceName: {}.",
            modsecRuleBlob,
            tenantID,
            serviceName,
            e);
      }
      return false;
    }
  }
}
