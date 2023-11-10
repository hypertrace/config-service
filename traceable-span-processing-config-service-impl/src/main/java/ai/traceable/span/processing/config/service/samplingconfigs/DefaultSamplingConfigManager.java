package ai.traceable.span.processing.config.service.samplingconfigs;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.store.SamplingConfigsConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SamplingConfigMetadata;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusException;
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
}
