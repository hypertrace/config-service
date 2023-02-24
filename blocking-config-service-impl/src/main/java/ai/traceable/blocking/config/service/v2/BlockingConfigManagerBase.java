package ai.traceable.blocking.config.service.v2;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingConfigManagerBase {
  List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      RequestContext requestContext,
      Optional<String> environmentId);
}
