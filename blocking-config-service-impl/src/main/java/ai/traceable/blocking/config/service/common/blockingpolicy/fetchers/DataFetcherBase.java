package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface DataFetcherBase {
  List<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext, Optional<String> environmentId);
}
