package ai.traceable.blocking.config.service.entity;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class EntityFetcher {

  public Optional<String> getEnvironmentId(RequestContext requestContext, String environment)
      throws ExecutionException {
    if (environment == null || environment.isEmpty()) {
      return Optional.empty();
    }
    // We are depending upon the fact that environment id is environment name for the current set of
    // environment entities
    // TODO: Fetch environment from entity service once environment id generation is changed
    return Optional.of(environment);
  }
}
