package ai.traceable.anomaly.config.service.common.license;

import ai.traceable.license.metering.service.api.v1.GetLicenseInfoRequest;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
@Singleton
public class LicenseInfoLoader {

  private final Duration callTimeout;
  private final LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub
      licenseMeteringServiceBlockingStub;

  private final LoadingCache<ContextualKey<Void>, LicenseInfo.Tier> licenseTierCache;

  @Inject
  public LicenseInfoLoader(
      LicenseMeteringServiceConfig config,
      LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub
          licenseMeteringServiceBlockingStub) {

    this.callTimeout = config.getCallTimeout();
    this.licenseMeteringServiceBlockingStub = licenseMeteringServiceBlockingStub;

    licenseTierCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(config.getCacheExpiryDuration())
            .maximumSize(config.getCacheMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadLicenseInfo));

    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        this.getClass().getName() + ".licenseTierCache",
        licenseTierCache,
        Collections.emptyMap(),
        config.getCacheMaxSize());
  }

  public LicenseInfo.Tier getLicenseTier(RequestContext requestContext) throws ExecutionException {
    return licenseTierCache.get(requestContext.buildInternalContextualKey());
  }

  private LicenseInfo.Tier loadLicenseInfo(ContextualKey<Void> contextKey) {
    try {
      return contextKey
          .getContext()
          .call(
              () ->
                  licenseMeteringServiceBlockingStub
                      .withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
                      .getLicenseInfo(GetLicenseInfoRequest.getDefaultInstance()))
          .getLicenseInfo()
          .getTier();
    } catch (Exception e) {
      log.error(
          String.format(
              "Unable to fetch license info for tenant: %s", contextKey.getContext().getTenantId()),
          e);
      return LicenseInfo.Tier.TIER_UNSPECIFIED;
    }
  }
}
