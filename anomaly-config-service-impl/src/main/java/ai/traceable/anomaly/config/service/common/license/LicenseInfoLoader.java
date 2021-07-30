package ai.traceable.anomaly.config.service.common.license;

import ai.traceable.license.metering.service.api.v1.GetLicenseInfoRequest;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LicenseInfoLoader {

  private final long callTimeoutInMs;
  private final LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub
      licenseMeteringServiceBlockingStub;

  private final LoadingCache<ContextualKey<Void>, LicenseInfo.Tier> licenseTierCache;

  @Inject
  public LicenseInfoLoader(
      LicenseMeteringServiceConfig config,
      LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub
          licenseMeteringServiceBlockingStub) {

    this.callTimeoutInMs = config.getCallTimeoutInMs();
    this.licenseMeteringServiceBlockingStub = licenseMeteringServiceBlockingStub;

    licenseTierCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(config.getCacheExpiryDuration())
            .maximumSize(config.getCacheMaxSize())
            .build(CacheLoader.from(this::loadLicenseInfo));
  }

  public LicenseInfo.Tier getLicenseTier(RequestContext requestContext) throws ExecutionException {
    return licenseTierCache.get(requestContext.buildContextualKey());
  }

  private LicenseInfo.Tier loadLicenseInfo(ContextualKey<Void> contextKey) {
    try {
      return contextKey
          .getContext()
          .call(
              () ->
                  licenseMeteringServiceBlockingStub
                      .withDeadlineAfter(callTimeoutInMs, TimeUnit.MILLISECONDS)
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
