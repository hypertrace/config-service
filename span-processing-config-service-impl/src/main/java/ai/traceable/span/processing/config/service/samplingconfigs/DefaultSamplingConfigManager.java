package ai.traceable.span.processing.config.service.samplingconfigs;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;

import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.span.processing.config.service.licensestatus.LicenseStatusConfigManager;
import ai.traceable.span.processing.config.service.store.SamplingConfigsConfigStore;
import ai.traceable.span.processing.config.service.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SamplingConfigMetadata;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.WindowedRateLimit;
import com.google.inject.Inject;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultSamplingConfigManager implements SamplingConfigManager {

  private static final String ZERO_SAMPLING_CONFIG_ID = "zero-sampling-config-id";
  private static final String DEFAULT_SAMPLING_CONFIG_ID = "default-sampling-config-id";

  private final TimestampConverter timestampConverter;
  private final RateLimitConfigManager rateLimitConfigManager;
  private final LicenseStatusConfigManager licenseStatusConfigManager;
  private final SamplingConfigsConfigStore samplingConfigsConfigStore;

  @Inject
  public DefaultSamplingConfigManager(
      SamplingConfigsConfigStore samplingConfigsConfigStore,
      TimestampConverter timestampConverter,
      RateLimitConfigManager rateLimitConfigManager,
      LicenseStatusConfigManager licenseStatusConfigManager) {
    this.timestampConverter = timestampConverter;
    this.samplingConfigsConfigStore = samplingConfigsConfigStore;
    this.rateLimitConfigManager = rateLimitConfigManager;
    this.licenseStatusConfigManager = licenseStatusConfigManager;
  }

  @Override
  public List<SamplingConfigDetails> getAllSamplingConfigsDetails(RequestContext requestContext) {
    return this.samplingConfigsConfigStore.getAllData(requestContext);
  }

  @Override
  public List<SamplingConfig> getAllResolvedSamplingConfigs(RequestContext requestContext) {
    LicenseStatus licenseStatus = licenseStatusConfigManager.getLicenseStatus();
    if (LICENSE_LIMIT_EXHAUSTED.equals(licenseStatus.getTracesLicenseLimit())) {
      return List.of(buildZeroSamplingConfig());
    }
    List<SamplingConfig> samplingConfigs = new ArrayList<>(getAllSamplingConfigs(requestContext));
    SamplingConfig defaultSamplingConfig = getDefaultSamplingConfig();
    samplingConfigs.add(defaultSamplingConfig);
    return Collections.unmodifiableList(samplingConfigs);
  }

  @Override
  public SamplingConfigDetails createSamplingConfig(
      RequestContext requestContext, CreateSamplingConfigRequest createSamplingConfigRequest) {
    // TODO: need to handle priorities
    SamplingConfig newSamplingConfig =
        SamplingConfig.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setSamplingConfigInfo(createSamplingConfigRequest.getSamplingConfigInfo())
            .build();
    return buildSamplingConfigDetails(
        this.samplingConfigsConfigStore.upsertObject(requestContext, newSamplingConfig));
  }

  @Override
  public SamplingConfigDetails updateSamplingConfig(
      RequestContext requestContext, UpdateSamplingConfigRequest updateSamplingConfigRequest)
      throws StatusException {
    // TODO: need to handle priorities
    UpdateSamplingConfig updateSamplingConfig = updateSamplingConfigRequest.getSamplingConfig();
    SamplingConfig existingSamplingConfig =
        this.samplingConfigsConfigStore
            .getData(requestContext, updateSamplingConfig.getId())
            .orElseThrow(Status.NOT_FOUND::asException);
    SamplingConfig updatedSamplingConfig =
        buildUpdatedSamplingConfig(existingSamplingConfig, updateSamplingConfig);
    return buildSamplingConfigDetails(
        this.samplingConfigsConfigStore.upsertObject(requestContext, updatedSamplingConfig));
  }

  @Override
  public void deleteSamplingConfig(
      RequestContext requestContext, DeleteSamplingConfigRequest deleteSamplingConfigRequest) {
    // TODO: need to handle priorities
    this.samplingConfigsConfigStore
        .deleteObject(requestContext, deleteSamplingConfigRequest.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private List<SamplingConfig> getAllSamplingConfigs(RequestContext requestContext) {
    return getAllSamplingConfigsDetails(requestContext).stream()
        .map(SamplingConfigDetails::getSamplingConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  private SamplingConfigDetails buildSamplingConfigDetails(
      ContextualConfigObject<SamplingConfig> configObject) {
    return SamplingConfigDetails.newBuilder()
        .setSamplingConfig(configObject.getData())
        .setMetadata(
            SamplingConfigMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private SamplingConfig buildUpdatedSamplingConfig(
      SamplingConfig existingSamplingConfig, UpdateSamplingConfig updateSamplingConfig) {
    return SamplingConfig.newBuilder(existingSamplingConfig)
        .setSamplingConfigInfo(
            SamplingConfigInfo.newBuilder()
                .setRateLimitConfig(updateSamplingConfig.getRateLimitConfig())
                .setFilter(updateSamplingConfig.getFilter())
                .build())
        .build();
  }

  private SamplingConfig buildZeroSamplingConfig() {
    return SamplingConfig.newBuilder()
        .setId(ZERO_SAMPLING_CONFIG_ID)
        .setSamplingConfigInfo(
            SamplingConfigInfo.newBuilder()
                .setRateLimitConfig(
                    RateLimitConfig.newBuilder()
                        .setTraceLimitGlobal(
                            RateLimit.newBuilder()
                                .setFixedWindowLimit(
                                    WindowedRateLimit.newBuilder()
                                        .setQuantityAllowed(0)
                                        .setWindowDuration(
                                            Duration.newBuilder().setSeconds(60).build())
                                        .build())
                                .build())
                        .setTraceLimitPerEndpoint(
                            RateLimit.newBuilder()
                                .setFixedWindowLimit(
                                    WindowedRateLimit.newBuilder()
                                        .setQuantityAllowed(0)
                                        .setWindowDuration(
                                            Duration.newBuilder().setSeconds(60).build())
                                        .build())
                                .build())
                        .setApiEndpointCacheDuration(
                            Duration.newBuilder().setSeconds(604800).build())
                        .build())
                .build())
        .build();
  }

  private SamplingConfig getDefaultSamplingConfig() {
    RateLimitConfig defaultRateLimitConfig = rateLimitConfigManager.getRateLimitConfig();
    return SamplingConfig.newBuilder()
        .setId(DEFAULT_SAMPLING_CONFIG_ID)
        .setSamplingConfigInfo(
            SamplingConfigInfo.newBuilder().setRateLimitConfig(defaultRateLimitConfig).build())
        .build();
  }
}
