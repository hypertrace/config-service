package ai.traceable.integration.config.service.wiz.store;

import static ai.traceable.integration.config.service.constants.IntegrationServiceConstants.INTEGRATION_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.integration.config.service.wiz.v1.WizIntegration;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class WizIntegrationConfigStore
    extends IdentifiedObjectStoreWithFilter<WizIntegration, WizIntegrationFilter> {

  private static final String WIZ_INTEGRATION_CONFIG_RESOURCE_NAME = "wiz-integration";

  @Inject
  public WizIntegrationConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        INTEGRATION_CONFIG_RESOURCE_NAMESPACE,
        WIZ_INTEGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<WizIntegration> buildDataFromValue(Value value) {
    try {
      WizIntegration.Builder wizIntegrationBuilder = WizIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, wizIntegrationBuilder);
      return Optional.of(wizIntegrationBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize wiz Integration from value -> {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(WizIntegration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(WizIntegration data) {
    return data.getId();
  }

  @Override
  protected Optional<WizIntegration> filterConfigData(
      WizIntegration data, WizIntegrationFilter filter) {
    if (filter.getIdsList().stream().anyMatch(id -> id.equals(data.getId()))) {
      return Optional.of(data);
    }
    return Optional.empty();
  }
}
