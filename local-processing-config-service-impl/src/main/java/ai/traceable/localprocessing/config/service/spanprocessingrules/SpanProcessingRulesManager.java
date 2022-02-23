package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;

public interface SpanProcessingRulesManager {
  GetSpanProcessingRulesResponse getSpanProcessingRulesResponse(
      GetSpanProcessingRulesRequest request);
}
