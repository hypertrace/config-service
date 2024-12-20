package ai.traceable.licensestatus.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseStatus.License.LicenseTier.LICENSE_TIER_ENTERPRISE;
import static ai.traceable.licensestatus.config.service.v1.LicenseStatus.License.LicenseTier.LICENSE_TIER_FREE;
import static ai.traceable.licensestatus.config.service.v1.LicenseStatus.License.LicenseTier.LICENSE_TIER_INTERNAL;
import static ai.traceable.licensestatus.config.service.v1.LicenseStatus.License.LicenseTier.LICENSE_TIER_TEAM;
import static ai.traceable.licensestatus.config.service.v1.LicenseStatus.License.LicenseTier.LICENSE_TIER_TRIAL;

import ai.traceable.license.metering.service.api.v1.GetLicensesRequest;
import ai.traceable.license.metering.service.api.v1.GetLicensesResponse;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub;
import ai.traceable.license.metering.service.api.v1.Licenses;
import ai.traceable.license.metering.service.api.v1.Licenses.Tier;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus.License;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus.License.LicenseTier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class LicenseProvider {
  private static final String CALL_TIMEOUT_CONFIG_NAME = "call.timeout.duration";
  private static final String CACHE_EXPIRATION_DURATION_CONFIG_NAME = "cache.expiration.duration";
  private static final String CACHE_MAX_SIZE_CONFIG_NAME = "cache.max.size";
  private static final String LICENSE_PROVIDER_CACHE_NAME = "LicenseProviderCache";

  private final LicenseMeteringServiceBlockingStub licenseMeteringServiceBlockingStub;
  private final LoadingCache<ContextualKey<Void>, Licenses> licenseCache;
  private final Duration requestTimeout;

  @Inject
  public LicenseProvider(
      Config licenseConfig, LicenseMeteringServiceBlockingStub licenseMeteringServiceBlockingStub) {
    this.licenseMeteringServiceBlockingStub = licenseMeteringServiceBlockingStub;
    this.requestTimeout = licenseConfig.getDuration(CALL_TIMEOUT_CONFIG_NAME);

    int maxCacheSize = licenseConfig.getInt(CACHE_MAX_SIZE_CONFIG_NAME);
    this.licenseCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(licenseConfig.getDuration(CACHE_EXPIRATION_DURATION_CONFIG_NAME))
            .maximumSize(maxCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadLicense));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        LICENSE_PROVIDER_CACHE_NAME, licenseCache, Collections.emptyMap(), maxCacheSize);
  }

  private Licenses loadLicense(ContextualKey<Void> contextualKey) {
    GetLicensesResponse licensesResponse =
        contextualKey.callInContext(
            () ->
                this.licenseMeteringServiceBlockingStub
                    .withDeadlineAfter(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .getLicenses(GetLicensesRequest.newBuilder().build()));
    return licensesResponse.getLicenses();
  }

  @SneakyThrows
  protected LicenseStatus getLicenseStatus(@Nonnull RequestContext requestContext) {
    Licenses licenses = this.licenseCache.get(requestContext.buildInternalContextualKey());
    LicenseStatus.Builder builder = LicenseStatus.newBuilder();
    if (licenses.hasProtectionLicense()) {
      builder.setProtectionLicense(
          License.newBuilder()
              .setLicenseTier(convertTier(licenses.getProtectionLicense().getTier()))
              .build());
    }

    if (licenses.hasApiCatalogLicense()) {
      builder.setApiCatalogLicense(
          License.newBuilder()
              .setLicenseTier(convertTier(licenses.getApiCatalogLicense().getTier()))
              .build());
    }
    builder.setTracesLicenseLimit(LicenseLimit.LICENSE_LIMIT_AVAILABLE);
    return builder.build();
  }

  private LicenseTier convertTier(Tier tier) {
    switch (tier) {
      case TIER_INTERNAL:
        return LICENSE_TIER_INTERNAL;
      case TIER_FREE:
        return LICENSE_TIER_FREE;
      case TIER_TRIAL:
        return LICENSE_TIER_TRIAL;
      case TIER_TEAM:
        return LICENSE_TIER_TEAM;
      case TIER_ENTERPRISE:
        return LICENSE_TIER_ENTERPRISE;
      default:
        throw new UnsupportedOperationException("Unsupported tier : " + tier);
    }
  }
}
