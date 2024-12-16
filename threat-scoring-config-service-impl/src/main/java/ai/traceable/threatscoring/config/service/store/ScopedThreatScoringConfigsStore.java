package ai.traceable.threatscoring.config.service.store;

import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigScope;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ScopedThreatScoringConfigsStore
    extends IdentifiedObjectStore<ScopedThreatScoringConfigs> {
  private static final String THREAT_SCORING_RESOURCE_NAME = "threat-scoring-config";
  public static final String DEFAULT_THREAT_SCORING_CONTEXT = "default";

  @Inject
  public ScopedThreatScoringConfigsStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      String threatScoringResourceNamespace) {
    super(
        configServiceBlockingStub,
        threatScoringResourceNamespace,
        THREAT_SCORING_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<ScopedThreatScoringConfigs> buildDataFromValue(Value value) {
    try {
      ScopedThreatScoringConfigs.Builder builder = ScopedThreatScoringConfigs.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing value {} into ScopedThreatScoringConfigs failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedThreatScoringConfigs data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(ScopedThreatScoringConfigs data) {
    return getContextFromData(data.getConfigScope());
  }

  public List<ScopedThreatScoringConfigs> fetchConfigsInContextOrder(
      RequestContext requestContext, List<String> contextsWithIncreasingPriority) {
    Map<String, ScopedThreatScoringConfigs> scopedThreatScoringConfigsMap =
        fetchConfigMap(requestContext);
    return contextsWithIncreasingPriority.stream()
        .map(scopedThreatScoringConfigsMap::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  public String getContextFromData(ThreatScoringConfigScope scope) {
    if (scope.hasEnvironmentScope()) {
      return scope.getEnvironmentScope().getEnvironmentId();
    } else {
      return DEFAULT_THREAT_SCORING_CONTEXT;
    }
  }

  private Map<String, ScopedThreatScoringConfigs> fetchConfigMap(RequestContext requestContext) {
    return getAllObjects(requestContext).stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ContextualConfigObject::getContext, ConfigObject::getData));
  }
}
