package ai.traceable.data.parsing.config.service.v1.store;

import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;
import ai.traceable.data.parsing.config.service.v1.DataParsingRule;
import ai.traceable.data.parsing.config.service.v1.GetDataParsingRulesFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DataParsingRuleStore extends IdentifiedObjectStore<DataParsingConfig> {
  private static final String DATA_PARSING_RULE_RESOURCE_NAME = "data-parsing-rule";
  private static final String DATA_PARSING_RESOURCE_NAMESPACE = "data-parsing";

  @Inject
  public DataParsingRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator changeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_PARSING_RESOURCE_NAMESPACE,
        DATA_PARSING_RULE_RESOURCE_NAME,
        changeEventGenerator);
  }

  public List<DataParsingConfig> getDataParsingConfigs(
      RequestContext ctx, GetDataParsingRulesFilter filter) {
    return getAllConfigData(ctx).stream()
        .filter(config -> matchesFilter(config, filter))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(DataParsingConfig config, GetDataParsingRulesFilter filter) {
    if (filter.getIdsCount() > 0 && !filter.getIdsList().contains(config.getId())) {
      return false;
    }
    DataParsingRule dataParsingRule = config.getDataParsingRule();
    if (filter.hasEnabled() && dataParsingRule.getEnabled() != filter.getEnabled()) {
      return false;
    }
    if (filter.getEnvironmentIdsCount() > 0) {
      if (dataParsingRule.hasEnvironmentScope()) {
        Set<String> environmentIds = new HashSet<>(filter.getEnvironmentIdsList());
        return dataParsingRule.getEnvironmentScope().getEnvironmentIdsList().stream()
            .anyMatch(environmentIds::contains);
      }
    }
    return true;
  }

  public List<DataParsingConfig> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected Optional<DataParsingConfig> buildDataFromValue(Value value) {
    DataParsingConfig.Builder builder = DataParsingConfig.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to DataParsingRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DataParsingConfig data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @SneakyThrows
  @Override
  protected String getContextFromData(DataParsingConfig data) {
    return data.getId();
  }
}
