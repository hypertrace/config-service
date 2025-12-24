package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.SecuritySchemeProvider.UserRoleSecurityScheme;
import ai.traceable.entity.fetcher.cache.SecuritySchemeProvider.UserScopeSecurityScheme;
import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider.HttpApiDetails;
import com.google.inject.Inject;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultStreamingSecuritySchemeProvider implements StreamingSecuritySchemeProvider {
  private final SecuritySchemeProvider securitySchemeProvider;
  private final EntityQueryServiceClient entityQueryServiceClient;

  @Override
  public Stream<ApiSecuritySchemeDetails> getAllSecuritySchemes(
      RequestContext requestContext, String serviceName, String environment) {

    Stream<HttpApiDetails> apis =
        entityQueryServiceClient.getAllHttpApiEndpoints(requestContext, serviceName, environment);

    return apis.map(
            api -> {
              try {
                Set<UserRoleSecurityScheme> roles =
                    securitySchemeProvider.getUserRoleSecuritySchemes(
                        requestContext, api.getApiId());
                if (roles.stream().noneMatch(UserRoleSecurityScheme::isUserDefined)) {
                  roles = Set.of();
                }
                Set<UserScopeSecurityScheme> scopes =
                    securitySchemeProvider.getUserScopeSecuritySchemes(
                        requestContext, api.getApiId());

                if (scopes.stream().noneMatch(UserScopeSecurityScheme::isUserDefined)) {
                  scopes = Set.of();
                }

                return new ApiSecuritySchemeDetails(api.getApiId(), roles, scopes);
              } catch (Exception e) {
                log.error(
                    "Error fetching security schemes for apiId: {} in service: {}",
                    api.getApiId(),
                    serviceName,
                    e);
                return new ApiSecuritySchemeDetails(api.getApiId(), Set.of(), Set.of());
              }
            })
        .filter(details -> !details.getUserRoles().isEmpty() || !details.getUserScopes().isEmpty());
  }
}
