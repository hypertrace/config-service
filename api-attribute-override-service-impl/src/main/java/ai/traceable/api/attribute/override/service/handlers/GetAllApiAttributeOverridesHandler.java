package ai.traceable.api.attribute.override.service.handlers;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesRequest;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class GetAllApiAttributeOverridesHandler {
  private final ConfigServiceHandler configServiceHandler;

  @Inject
  GetAllApiAttributeOverridesHandler(ConfigServiceHandler configServiceHandler) {
    this.configServiceHandler = configServiceHandler;
  }

  public Map<String, ApiAttributeOverrides> getOverrides(
      GetAllApiAttributeOverridesRequest request, RequestContext requestContext) {
    if (!request.hasFilter()) {
      return configServiceHandler.getAllApiAttributeOverrides(requestContext);
    }

    Set<String> requiredApiIds = new HashSet<>(request.getFilter().getApiIdsList());
    return configServiceHandler.getAllApiAttributeOverrides(requestContext).entrySet().stream()
        .filter(e -> requiredApiIds.contains(e.getKey()))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }
}
