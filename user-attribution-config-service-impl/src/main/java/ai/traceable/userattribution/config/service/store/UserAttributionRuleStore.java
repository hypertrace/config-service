package ai.traceable.userattribution.config.service.store;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class UserAttributionRuleStore extends IdentifiedObjectStore<UserAttributionRule> {
  private static final String USER_ATTRIBUTION_RULE_RESOURCE_NAME = "user-attribution-rule";
  private static final String USER_ATTRIBUTION_RESOURCE_NAMESPACE = "user-attribution";

  private final RankCalculator<UserAttributionRule, String> rankCalculator;

  @Inject
  public UserAttributionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RankCalculator<UserAttributionRule, String> rankCalculator) {
    super(
        configServiceBlockingStub,
        USER_ATTRIBUTION_RESOURCE_NAMESPACE,
        USER_ATTRIBUTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
    this.rankCalculator = rankCalculator;
  }

  public List<UserAttributionRule> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected Optional<UserAttributionRule> buildDataFromValue(Value value) {
    UserAttributionRule.Builder builder = UserAttributionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to UserAttributionRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(UserAttributionRule data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(UserAttributionRule data) {
    return data.getId();
  }

  @Override
  protected List<ContextualConfigObject<UserAttributionRule>> orderFetchedObjects(
      List<ContextualConfigObject<UserAttributionRule>> objects) {
    return this.rankCalculator.orderFromRanks(objects, ContextualConfigObject::getData);
  }
}
