package ai.traceable.localprocessing.config.service.apinaming;

import ai.traceable.localprocessing.config.service.v1.GetApiNamingRequest;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiNamingManager {
  List<HttpServiceResponse> getHttpServiceResponseList(
      RequestContext requestContext, GetApiNamingRequest request);

  List<String> getFallbackWildcardRegexes();
}
