package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ApiSecurityScheme;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.config.service.v1.SecuritySchemeConfig;
import ai.traceable.edge.config.service.v1.UserRoleScheme;
import ai.traceable.edge.config.service.v1.UserScopeScheme;
import ai.traceable.entity.fetcher.cache.StreamingSecuritySchemeProvider;
import ai.traceable.entity.fetcher.cache.StreamingSecuritySchemeProvider.ApiSecuritySchemeDetails;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.Map;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class SecuritySchemeConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = "SecuritySchemeConfig";
  private static final String SERVICE_NAME = "serviceName";
  private static final String ENVIRONMENT = "environment";

  private final StreamingSecuritySchemeProvider securitySchemeProvider;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @SneakyThrows
  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {

    Map<String, String> additionalFields = agentCapabilities.getAdditionalFieldsMap();

    if (!additionalFields.containsKey(SERVICE_NAME)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing serviceName in agent capabilities")
          .asRuntimeException();
    }

    String serviceName = additionalFields.get(SERVICE_NAME);
    ConfigPayloads configPayloads = buildConfigPayloads(requestContext, serviceName, environment);

    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setConfigPayloads(configPayloads)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setHash(uuidGenerator.generateId(configPayloads))
        .setEnabled(true)
        .build();
  }

  @NonNull
  private ConfigPayloads buildConfigPayloads(
      RequestContext requestContext, String serviceName, String environment) {

    assertNonNullOrEmpty(serviceName, SERVICE_NAME);
    assertNonNullOrEmpty(environment, ENVIRONMENT);

    Stream<ApiSecuritySchemeDetails> apiSchemes =
        securitySchemeProvider.getAllSecuritySchemes(requestContext, serviceName, environment);

    SecuritySchemeConfig.Builder securitySchemeConfigBuilder = SecuritySchemeConfig.newBuilder();

    apiSchemes.forEach(
        apiScheme -> {
          ApiSecurityScheme.Builder apiSecuritySchemeBuilder = ApiSecurityScheme.newBuilder();
          apiScheme
              .getUserRoles()
              .forEach(
                  role ->
                      apiSecuritySchemeBuilder.addUserRoles(
                          UserRoleScheme.newBuilder()
                              .setRoleId(role.getRoleId())
                              .setRoleName(role.getRoleName())
                              .setIsUserDefined(role.isUserDefined())
                              .build()));

          apiScheme
              .getUserScopes()
              .forEach(
                  scope ->
                      apiSecuritySchemeBuilder.addUserScopes(
                          UserScopeScheme.newBuilder()
                              .setScopeId(scope.getScopeId())
                              .setScopeName(scope.getScopeName())
                              .setIsUserDefined(scope.isUserDefined())
                              .build()));

          securitySchemeConfigBuilder.putApiSecuritySchemes(
              apiScheme.getApiId(), apiSecuritySchemeBuilder.build());
        });

    SecuritySchemeConfig data = securitySchemeConfigBuilder.build();

    if (data.getApiSecuritySchemesMap().isEmpty()) {
      return ConfigPayloads.getDefaultInstance();
    }

    return ConfigPayloads.newBuilder().addConfigBytes(data.toByteString()).build();
  }

  private static void assertNonNullOrEmpty(String value, String fieldName) {
    if (value == null || value.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("%s is null or empty", fieldName))
          .asRuntimeException();
    }
  }
}
