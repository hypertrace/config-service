package ai.traceable.alerting.config.service;

import static ai.traceable.alerting.config.service.v2.SecurityConfigurationType.SECURITY_CONFIGURATION_TYPE_THREAT_AUTO_BLOCKING;
import static ai.traceable.alerting.config.service.v2.ThreatScoringConfigType.THREAT_SCORING_CONFIG_TYPE_THREAT_AUTO_BLOCKING;

import ai.traceable.alerting.config.service.v2.EventCondition;
import ai.traceable.alerting.config.service.v2.EventConditionMutableData;
import ai.traceable.alerting.config.service.v2.SecurityConfigChangeEventCondition;
import ai.traceable.alerting.config.service.v2.SecurityConfigurationType;
import ai.traceable.alerting.config.service.v2.ThreatScoringConfigChangeEventCondition;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.NotificationRuleFilteredStore;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;

@Slf4j
public class EventConditionStore extends IdentifiedObjectStore<EventCondition> {

  private static final String MIGRATION_MESSAGE = "Migrated - ";

  public static final String ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME =
      "alertingEventConditionConfig";
  public static final String ALERTING_CONFIG_NAMESPACE = "alerting-v1";

  private final UuidGenerator uuidGenerator;
  private final NotificationRuleFilteredStore notificationRuleStore;
  private final FeatureCachingClient featureCachingClient;
  private static final Set<ContextualKey<Void>> PROCESSED_RULE_TENANT_IDS = new HashSet<>();

  public EventConditionStore(
      Channel configChannel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    super(
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        ALERTING_CONFIG_NAMESPACE,
        ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME);
    this.uuidGenerator = new UuidGenerator();
    this.notificationRuleStore =
        new NotificationRuleFilteredStore(configChannel, configChangeEventGenerator);
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public List<EventCondition> getAllConfigData(RequestContext requestContext) {
    List<EventCondition> eventConditions = super.getAllConfigData(requestContext);
    if (this.featureCachingClient.isThreatScoringNotificationRuleMigrationEnabled(requestContext)
        && !PROCESSED_RULE_TENANT_IDS.contains(requestContext.buildInternalContextualKey())) {
      List<EventCondition> migratedEventConditions =
          migrateEventConditionWithThreatAutoBlockingConfigs(requestContext, eventConditions);
      PROCESSED_RULE_TENANT_IDS.add(requestContext.buildInternalContextualKey());
      return migratedEventConditions;
    }
    return eventConditions;
  }

  @Override
  protected Optional<EventCondition> buildDataFromValue(Value value) {
    EventCondition.Builder builder = EventCondition.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EventCondition data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(EventCondition data) {
    return data.getId();
  }

  private List<EventCondition> migrateEventConditionWithThreatAutoBlockingConfigs(
      RequestContext requestContext, List<EventCondition> currentEventConditions) {
    Map<String, NotificationRule> eventConditionIdToNotificationRuleMap =
        notificationRuleStore.getAllConfigData(requestContext).stream()
            .collect(
                Collectors.toMap(
                    rule -> rule.getNotificationRuleMutableData().getEventConditionId(),
                    rule -> rule));
    List<EventCondition> eventConditions = new ArrayList<>();

    for (EventCondition eventCondition : currentEventConditions) {
      NotificationRule notificationRule =
          eventConditionIdToNotificationRuleMap.get(eventCondition.getId());
      if (eventCondition
          .getEventConditionMutableData()
          .getSecurityConfigChangeEventCondition()
          .getSecurityConfigurationTypesList()
          .contains(SECURITY_CONFIGURATION_TYPE_THREAT_AUTO_BLOCKING)) {
        if (eventCondition
                .getEventConditionMutableData()
                .getSecurityConfigChangeEventCondition()
                .getSecurityConfigurationTypesList()
                .size()
            == 1) {
          // deleting eventCondition and notification rule for migration, where it has only
          // SECURITY_CONFIGURATION_TYPE_THREAT_AUTO_BLOCKING.
          if (Objects.nonNull(notificationRule)) {
            notificationRuleStore
                .deleteObject(requestContext, notificationRule.getId())
                .orElseThrow(Status.NOT_FOUND::asRuntimeException);
          }
          super.deleteObject(requestContext, eventCondition.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
        } else {
          migrateSecurityConfigEventCondition(requestContext, eventCondition, eventConditions);
        }
        if (Objects.nonNull(notificationRule)) {
          migrateNotificationRule(
              requestContext, eventCondition, notificationRule, eventConditions);
        }
      } else {
        eventConditions.add(eventCondition);
      }
    }
    return eventConditions;
  }

  private void migrateSecurityConfigEventCondition(
      RequestContext requestContext,
      EventCondition eventCondition,
      List<EventCondition> migratedEventConditions) {
    // removing SECURITY_CONFIGURATION_TYPE_THREAT_AUTO_BLOCKING from event condition
    List<SecurityConfigurationType> migratedSecurityConfigurationTypes =
        eventCondition
            .getEventConditionMutableData()
            .getSecurityConfigChangeEventCondition()
            .getSecurityConfigurationTypesList()
            .stream()
            .filter(
                securityConfigurationType ->
                    !securityConfigurationType.equals(
                        SECURITY_CONFIGURATION_TYPE_THREAT_AUTO_BLOCKING))
            .collect(Collectors.toList());
    SecurityConfigChangeEventCondition migratedSecurityConfigurationEventCondition =
        eventCondition
            .getEventConditionMutableData()
            .getSecurityConfigChangeEventCondition()
            .toBuilder()
            .clearSecurityConfigurationTypes()
            .addAllSecurityConfigurationTypes(migratedSecurityConfigurationTypes)
            .build();
    EventConditionMutableData migratedEventConditionMutableData =
        eventCondition.getEventConditionMutableData().toBuilder()
            .setSecurityConfigChangeEventCondition(migratedSecurityConfigurationEventCondition)
            .build();
    EventCondition migratedEventCondition =
        eventCondition.toBuilder()
            .setEventConditionMutableData(migratedEventConditionMutableData)
            .build();
    migratedEventConditions.add(
        super.upsertObject(requestContext, migratedEventCondition).getData());
  }

  private void migrateNotificationRule(
      RequestContext requestContext,
      EventCondition eventCondition,
      NotificationRule notificationRule,
      List<EventCondition> migratedEventConditions) {
    EventCondition threatScoringEventCondition =
        getThreatScoringEvenCondition(requestContext, eventCondition);

    NotificationRuleMutableData notificationRuleMutableData =
        notificationRule.getNotificationRuleMutableData();
    NotificationRuleMutableData migratedNotificationRuleMutableData =
        notificationRuleMutableData.toBuilder()
            .setRuleName(MIGRATION_MESSAGE + notificationRuleMutableData.getRuleName())
            .setDescription(MIGRATION_MESSAGE + notificationRuleMutableData.getDescription())
            .setEventConditionId(threatScoringEventCondition.getId())
            .setEventConditionType(
                threatScoringEventCondition
                    .getEventConditionMutableData()
                    .getConditionCase()
                    .toString())
            .build();

    NotificationRule migrateNotificationRule =
        NotificationRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setNotificationRuleMutableData(migratedNotificationRuleMutableData)
            .build();
    notificationRuleStore.upsertObject(requestContext, migrateNotificationRule);
    migratedEventConditions.add(threatScoringEventCondition);
  }

  private EventCondition getThreatScoringEvenCondition(
      RequestContext requestContext, EventCondition eventCondition) {
    // creating THREAT_SCORING_CONFIG_TYPE_THREAT_AUTO_BLOCKING event condition for migration.
    EventConditionMutableData.Builder builder =
        EventConditionMutableData.newBuilder()
            .setThreatScoringConfigChangeEventCondition(
                ThreatScoringConfigChangeEventCondition.newBuilder()
                    .addThreatScoringConfigTypes(THREAT_SCORING_CONFIG_TYPE_THREAT_AUTO_BLOCKING));
    if (eventCondition.getEventConditionMutableData().hasScope()) {
      builder.setScope(eventCondition.getEventConditionMutableData().getScope());
    }
    EventCondition threatScoringEventCondition =
        EventCondition.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setEventConditionMutableData(builder)
            .build();
    return super.upsertObject(requestContext, threatScoringEventCondition).getData();
  }
}
