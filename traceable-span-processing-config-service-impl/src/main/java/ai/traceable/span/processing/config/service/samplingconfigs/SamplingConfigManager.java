package ai.traceable.span.processing.config.service.samplingconfigs;

import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import io.grpc.StatusException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SamplingConfigManager {
  List<SamplingConfigDetails> getAllSamplingConfigsDetails(RequestContext requestContext);

  List<SamplingConfig> getAllResolvedSamplingConfigs(RequestContext requestContext);

  SamplingConfigDetails createSamplingConfig(
      RequestContext requestContext, CreateSamplingConfigRequest createSamplingConfigRequest);

  SamplingConfigDetails updateSamplingConfig(
      RequestContext requestContext, UpdateSamplingConfigRequest updateSamplingConfigRequest)
      throws StatusException;

  void deleteSamplingConfig(
      RequestContext requestContext, DeleteSamplingConfigRequest deleteSamplingConfigRequest);
}
