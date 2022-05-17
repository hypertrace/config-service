package ai.traceable.external.data.classification.config.service;

import ai.traceable.sensitivedata.config.service.v1.Parameter;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

interface InsightsServiceCoordinator {
  List<Parameter> getSensitiveHeaderParameters(RequestContext requestContext);
}
