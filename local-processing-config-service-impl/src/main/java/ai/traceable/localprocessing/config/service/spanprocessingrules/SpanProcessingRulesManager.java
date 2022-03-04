package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SpanProcessingRulesManager {
  GetSpanProcessingRulesResponse getSpanProcessingRulesResponse(
      RequestContext requestContext, GetSpanProcessingRulesRequest request);
}
