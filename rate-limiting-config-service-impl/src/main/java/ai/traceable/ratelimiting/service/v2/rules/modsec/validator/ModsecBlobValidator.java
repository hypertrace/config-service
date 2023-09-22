package ai.traceable.ratelimiting.service.v2.rules.modsec.validator;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.RuleEngine;
import ai.traceable.ratelimiting.service.v2.rules.modsec.EnrichedRateLimitingModsecRule;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class ModsecBlobValidator {
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);
  private static final boolean LOAD_NATIVE_LIBRARY_SUCCESS = loadNativeRuleEngineLibrary();
  private static final ConcurrentHashMap<String, Boolean> modsecBlobCache =
      new ConcurrentHashMap<>();

  private final UuidGenerator uuidGenerator;

  @Inject
  public ModsecBlobValidator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  private static boolean loadNativeRuleEngineLibrary() {
    try {
      RuleEngine.loadNativeLibrary();
      return true;
    } catch (IOException e) {
      log.warn(
          "Failed loading rule engine native library in rate-limiting rule modsec converted with error:",
          e);
      return false;
    }
  }

  public boolean validate(
      String modsecRuleBlob,
      String tenantId,
      List<String> serviceNames,
      List<String> environmentIds,
      Collection<EnrichedRateLimitingModsecRule> enrichedRateLimitingModsecRules) {
    if (modsecRuleBlob.isBlank()) {
      return true;
    }

    String modsecBlobHash = uuidGenerator.generateId(modsecRuleBlob);
    if (!modsecBlobCache.containsKey(modsecBlobHash)) {
      log.debug(
          "For customer ID: {}, Created modsec blob: [{}] for the enrichedRateLimitingModsecRules: [{}]",
          tenantId,
          modsecRuleBlob,
          enrichedRateLimitingModsecRules);
      modsecBlobCache.put(
          modsecBlobHash,
          validateModsecBlob(modsecRuleBlob, tenantId, serviceNames, environmentIds));
    }
    return modsecBlobCache.get(modsecBlobHash);
  }

  private boolean validateModsecBlob(
      String modsecRuleBlob,
      String tenantName,
      List<String> serviceNames,
      List<String> environmentIds) {
    if (!LOAD_NATIVE_LIBRARY_SUCCESS) {
      return true;
    }

    try {
      RuleEngine ruleEngine = RuleEngine.create(modsecRuleBlob);
      RuleEngine.destroy(ruleEngine);
      return true;
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Invalid modsec rule was formed when trying to convert rate-limiting rule for tenant:{}, services:{} and environment: {}. Skipping.",
            tenantName,
            serviceNames,
            environmentIds,
            e);
      } else {
        log.debug(
            "Invalid modsec rule: {} was formed when trying to convert rate-limiting rule for tenant:{} and services:{}. Skipping.",
            modsecRuleBlob,
            tenantName,
            serviceNames,
            e);
      }
      return false;
    }
  }
}
