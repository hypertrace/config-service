package ai.traceable.localprocessing.config.service.apinaming;

import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.ServiceResponse;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiNamingManager {
  List<ServiceResponse> getServiceResponseList(
      RequestContext requestContext, GetApiNamingModelRequest request);

  List<String> getFallbackWildcardRegexes();
}
