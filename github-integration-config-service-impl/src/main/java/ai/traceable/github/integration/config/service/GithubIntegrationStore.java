package ai.traceable.github.integration.config.service;

import ai.traceable.github.integration.config.service.v1.GithubIntegration;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

class GithubIntegrationStore
    extends IdentifiedObjectStoreWithFilter<GithubIntegration, GithubIntegrationFilter> {
  private static final String RESOURCE_NAMESPACE = "githubIntegrationConfig";
  private static final String RESOURCE_NAME = "githubIntegration";

  @Inject
  public GithubIntegrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, RESOURCE_NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<GithubIntegration> buildDataFromValue(Value value) {
    GithubIntegration.Builder builder = GithubIntegration.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(GithubIntegration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(GithubIntegration data) {
    return data.getId();
  }

  @Override
  protected Optional<GithubIntegration> filterConfigData(
      GithubIntegration data, GithubIntegrationFilter filter) {
    return Optional.of(data)
        .filter(githubIntegration -> this.satisfiesAnyOwnerFilter(filter, githubIntegration));
  }

  private boolean satisfiesAnyOwnerFilter(
      GithubIntegrationFilter filter, GithubIntegration integration) {
    return !filter.hasInstallationOwnerName()
        || filter
            .getInstallationOwnerName()
            .equalsIgnoreCase(integration.getInstallationOwnerName());
  }
}
