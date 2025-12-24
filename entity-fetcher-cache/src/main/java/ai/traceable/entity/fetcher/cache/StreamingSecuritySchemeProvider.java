package ai.traceable.entity.fetcher.cache;

import java.util.Set;
import java.util.stream.Stream;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface StreamingSecuritySchemeProvider {
  Stream<ApiSecuritySchemeDetails> getAllSecuritySchemes(
      RequestContext requestContext, String serviceName, String environment);

  @Value
  class ApiSecuritySchemeDetails {
    String apiId;
    Set<SecuritySchemeProvider.UserRoleSecurityScheme> userRoles;
    Set<SecuritySchemeProvider.UserScopeSecurityScheme> userScopes;
  }
}
