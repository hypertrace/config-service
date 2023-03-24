package ai.traceable.ast.hooks.config.service.handlers;

import ai.traceable.ast.hooks.config.service.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import java.util.UUID;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class CreateAstHookHandler {
  private final AstHooksConfigStore configStore;

  public AstHook createHook(CreateAstHookRequest request, RequestContext requestContext) {
    String id = UUID.randomUUID().toString();
    AstHook astHook =
        AstHook.newBuilder().setId(id).setHookDetails(request.getHookDetails()).build();
    return configStore.upsertObject(requestContext, astHook).getData();
  }
}
