package ai.traceable.ast.hooks.config.service.handlers;

import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Optional;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class CreateAstHookHandler {
  private final UuidGenerator uuidGenerator;
  private final AstHooksConfigStore configStore;

  public AstHook createHook(CreateAstHookRequest request, RequestContext requestContext) {
    String id = uuidGenerator.generateRandomId();
    AstHook.Builder astHookBuilder =
        AstHook.newBuilder().setId(id).setHookDetails(request.getHookDetails());
    getRequestUser(requestContext).ifPresent(astHookBuilder::setCreatedBy);

    return configStore.upsertObject(requestContext, astHookBuilder.build()).getData();
  }

  private Optional<String> getRequestUser(RequestContext requestContext) {
    if (requestContext.getName().isPresent()) {
      return requestContext.getName();
    } else if (requestContext.getEmail().isPresent()) {
      return requestContext.getEmail();
    }
    return Optional.empty();
  }
}
