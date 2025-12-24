package ai.traceable.entity.fetcher.cache;

import java.util.Set;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SecuritySchemeProvider {
  Set<UserRoleSecurityScheme> getUserRoleSecuritySchemes(
      RequestContext requestContext, String apiId);

  Set<UserScopeSecurityScheme> getUserScopeSecuritySchemes(
      RequestContext requestContext, String apiId);

  @Value
  class UserRoleSecurityScheme {
    String roleId;
    String roleName;
    boolean isUserDefined;
  }

  @Value
  class UserScopeSecurityScheme {
    String scopeId;
    String scopeName;
    boolean isUserDefined;
  }
}
