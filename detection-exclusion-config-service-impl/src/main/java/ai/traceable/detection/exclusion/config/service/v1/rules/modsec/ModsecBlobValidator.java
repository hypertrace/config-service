package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
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
          "For customer ID: {}, Created modsec blob: [{}] for the exclusion rules for service: [{}] which failed validation",
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
      Status status = ModsecRuleEngineUtils.validate(modsecRuleBlob);
      return status.isOk() || status.equals(Status.UNKNOWN);
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Invalid modsec rule was formed when trying to convert exclusion rule for tenant:{}, service:{}. Skipping.",
            tenantName,
            serviceName,
            e);
      } else {
        log.debug(
            "Invalid modsec rule: {} was formed when trying to convert exclusion rule for tenant:{} and service:{}. Skipping.",
            modsecRuleBlob,
            tenantName,
            serviceName,
            e);
      }
      return false;
    }
  }
}
