package ai.traceable.span.processing.config.service.samplingconfigs;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.store.SamplingConfigsConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.PercentageLimitConfig;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RateLimitStrategy;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SamplingConfigMetadata;
import ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class DefaultSamplingConfigManager implements SamplingConfigManager {

  private final TimestampConverter timestampConverter;
  private final SamplingConfigsConfigStore samplingConfigsConfigStore;

  @Override
  public List<SamplingConfigDetails> getAllSamplingConfigsDetails(RequestContext requestContext) {
    return this.samplingConfigsConfigStore.getAllData(requestContext);
  }

  @Override
  public List<SamplingConfig> getAllResolvedSamplingConfigs(RequestContext requestContext) {
    // A zero sampling config should be applied ideally if the license limit is exhausted. Since we
    // don't maintain licensing properly today, allowing them to be applied always
    return getAllSamplingConfigs(requestContext);
  }

  @Override
  public SamplingConfigDetails createSamplingConfig(
      RequestContext requestContext, CreateSamplingConfigRequest createSamplingConfigRequest) {
    // TODO: need to handle priorities
    SamplingConfigInfo samplingConfigInfo =
        syncRateLimitStrategies(
            truncatePercentage(createSamplingConfigRequest.getSamplingConfigInfo()));
    SamplingConfig newSamplingConfig =
        SamplingConfig.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setSamplingConfigInfo(samplingConfigInfo)
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
    SamplingConfig.Builder samplingConfigBuilder =
        SamplingConfig.newBuilder(existingSamplingConfig);
    SamplingConfigInfo.Builder samplingConfigInfoBuilder =
        SamplingConfigInfo.newBuilder(existingSamplingConfig.getSamplingConfigInfo());
    if (updateSamplingConfig.hasRateLimitConfig()) {
      samplingConfigInfoBuilder.setRateLimitConfig(
          syncRateLimitStrategies(updateSamplingConfig.getRateLimitConfig()));
    }
    if (updateSamplingConfig.hasFilter()) {
      samplingConfigInfoBuilder.setFilter(updateSamplingConfig.getFilter());
    }
    if (updateSamplingConfig.hasPercentageLimitConfig()) {
      samplingConfigInfoBuilder.setPercentageLimitConfig(
          truncatePercentage(updateSamplingConfig.getPercentageLimitConfig()));
    }
    return samplingConfigBuilder.setSamplingConfigInfo(samplingConfigInfoBuilder.build()).build();
  }

  private SamplingConfigInfo truncatePercentage(SamplingConfigInfo samplingConfigInfo) {
    if (!samplingConfigInfo.hasPercentageLimitConfig()) {
      return samplingConfigInfo;
    }
    return samplingConfigInfo.toBuilder()
        .setPercentageLimitConfig(truncatePercentage(samplingConfigInfo.getPercentageLimitConfig()))
        .build();
  }

  private PercentageLimitConfig truncatePercentage(PercentageLimitConfig config) {
    float truncated =
        BigDecimal.valueOf(config.getAllowedPercentage())
            .setScale(2, RoundingMode.DOWN)
            .floatValue();
    return config.toBuilder().setAllowedPercentage(truncated).build();
  }

  private SamplingConfigInfo syncRateLimitStrategies(SamplingConfigInfo samplingConfigInfo) {
    if (!samplingConfigInfo.hasRateLimitConfig()) {
      return samplingConfigInfo;
    }
    return samplingConfigInfo.toBuilder()
        .setRateLimitConfig(syncRateLimitStrategies(samplingConfigInfo.getRateLimitConfig()))
        .build();
  }

  private RateLimitConfig syncRateLimitStrategies(RateLimitConfig rateLimitConfig) {
    RateLimitStrategy rateLimitStrategy = rateLimitConfig.getRateLimitStrategy();
    SpanLimitingStrategy spanLimitStrategy = rateLimitConfig.getSpanLimitStrategy();

    boolean hasRateLimitStrategy =
        rateLimitStrategy != RateLimitStrategy.RATE_LIMIT_STRATEGY_UNSPECIFIED;
    boolean hasSpanLimitStrategy =
        spanLimitStrategy != SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_UNSPECIFIED;

    RateLimitConfig.Builder builder = rateLimitConfig.toBuilder();

    if (hasRateLimitStrategy && !hasSpanLimitStrategy) {
      builder.setSpanLimitStrategy(toSpanLimitingStrategy(rateLimitStrategy));
    } else if (hasSpanLimitStrategy && !hasRateLimitStrategy) {
      builder.setRateLimitStrategy(toRateLimitStrategy(spanLimitStrategy));
    }

    return builder.build();
  }

  private SpanLimitingStrategy toSpanLimitingStrategy(RateLimitStrategy rateLimitStrategy) {
    switch (rateLimitStrategy) {
      case RATE_LIMIT_STRATEGY_DROP:
        return SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_DROP;
      case RATE_LIMIT_STRATEGY_BARESPAN:
        return SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_BARESPAN;
      default:
        return SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_UNSPECIFIED;
    }
  }

  private RateLimitStrategy toRateLimitStrategy(SpanLimitingStrategy spanLimitingStrategy) {
    switch (spanLimitingStrategy) {
      case SPAN_LIMITING_STRATEGY_DROP:
        return RateLimitStrategy.RATE_LIMIT_STRATEGY_DROP;
      case SPAN_LIMITING_STRATEGY_BARESPAN:
        return RateLimitStrategy.RATE_LIMIT_STRATEGY_BARESPAN;
      default:
        return RateLimitStrategy.RATE_LIMIT_STRATEGY_UNSPECIFIED;
    }
  }
}
