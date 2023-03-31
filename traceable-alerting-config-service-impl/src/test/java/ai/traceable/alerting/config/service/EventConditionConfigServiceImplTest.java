package ai.traceable.alerting.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import ai.traceable.alerting.config.service.v2.*;
import ai.traceable.alerting.config.service.v2.EventConditionConfigServiceGrpc.EventConditionConfigServiceBlockingStub;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventConditionConfigServiceImplTest {

  EventConditionConfigServiceBlockingStub eventConditionsStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGetAll().mockDelete();

    this.mockGenericConfigService
        .addService(new EventConditionConfigServiceImpl(this.mockGenericConfigService.channel()))
        .start();

    this.eventConditionsStub =
        EventConditionConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void createReadUpdateReadDelete() {

    EventConditionMutableData eventConditionMutableData1 = getEventConditionMutableData();
    EventCondition eventCondition1 =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder()
                    .setEventConditionMutableData(eventConditionMutableData1)
                    .build())
            .getEventCondition();
    assertEquals(
        eventConditionMutableData1.getConditionCase().name(),
        eventCondition1.getEventConditionMutableData().getConditionCase().name());
    assertFalse(eventCondition1.getId().isEmpty());

    EventCondition eventCondition2 =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder()
                    .setEventConditionMutableData(eventConditionMutableData1)
                    .build())
            .getEventCondition();
    assertIterableEquals(
        List.of(eventCondition2, eventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionsList());

    // update severity of first event from HIGH -> LOW and second event from HIGH -> CRITICAL
    EventCondition eventCondition1ToUpdate =
        eventCondition1.toBuilder()
            .setEventConditionMutableData(
                EventConditionMutableData.newBuilder()
                    .setScope(
                        EventConditionScope.newBuilder()
                            .setEnvironmentScope(
                                EventConditionScope.EnvironmentScope.newBuilder()
                                    .addAllEnvironmentNames(List.of("env1", "env2"))
                                    .build())
                            .build())
                    .setDetectedSecurityEventCondition(
                        DetectedSecurityEventCondition.newBuilder()
                            .addAllEventTypes(
                                Collections.singletonList(
                                    SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                            .addAllSeverities(
                                Collections.singletonList(
                                    SecurityEventSeverity.SECURITY_EVENT_SEVERITY_LOW))
                            .build()))
            .build();
    EventCondition eventCondition2ToUpdate =
        eventCondition2.toBuilder()
            .setEventConditionMutableData(
                EventConditionMutableData.newBuilder()
                    .setScope(
                        EventConditionScope.newBuilder()
                            .setEnvironmentScope(
                                EventConditionScope.EnvironmentScope.newBuilder()
                                    .addAllEnvironmentNames(List.of("env1", "env2"))
                                    .build())
                            .build())
                    .setDetectedSecurityEventCondition(
                        DetectedSecurityEventCondition.newBuilder()
                            .addAllEventTypes(
                                Collections.singletonList(
                                    SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                            .addAllSeverities(
                                Collections.singletonList(
                                    SecurityEventSeverity.SECURITY_EVENT_SEVERITY_CRITICAL))
                            .build()))
            .build();

    EventCondition updatedEventCondition1 =
        eventConditionsStub
            .updateEventCondition(
                UpdateEventConditionRequest.newBuilder()
                    .setId(eventCondition1ToUpdate.getId())
                    .setEventConditionMutableData(
                        eventCondition1ToUpdate.getEventConditionMutableData())
                    .build())
            .getEventCondition();
    EventCondition updatedEventCondition2 =
        eventConditionsStub
            .updateEventCondition(
                UpdateEventConditionRequest.newBuilder()
                    .setId(eventCondition2ToUpdate.getId())
                    .setEventConditionMutableData(
                        eventCondition2ToUpdate.getEventConditionMutableData())
                    .build())
            .getEventCondition();

    assertEquals(eventCondition1ToUpdate, updatedEventCondition1);
    assertEquals(eventCondition2ToUpdate, updatedEventCondition2);

    assertIterableEquals(
        List.of(updatedEventCondition2, updatedEventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionsList());

    eventConditionsStub.deleteEventCondition(
        DeleteEventConditionRequest.newBuilder()
            .setEventConditionId(updatedEventCondition2.getId())
            .build());

    assertIterableEquals(
        List.of(updatedEventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionsList());
  }

  @Test
  public void when_EnvIsNotEmpty_And_EnvScopeListINotEmpty_ThenExpectEnvScopeListOnRead() {
    EventConditionMutableData data1 =
        EventConditionMutableData.newBuilder()
            .setDetectedSecurityEventCondition(
                DetectedSecurityEventCondition.newBuilder()
                    .addAllEventTypes(
                        Collections.singletonList(
                            SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                    .addAllSeverities(
                        Collections.singletonList(
                            SecurityEventSeverity.SECURITY_EVENT_SEVERITY_HIGH))
                    .build())
            .setEnvironment("env3")
            .build();

    // create with env field set
    EventCondition actual1 =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder()
                    .setEventConditionMutableData(data1)
                    .build())
            .getEventCondition();

    EventConditionMutableData data2 =
        EventConditionMutableData.newBuilder()
            .setDetectedSecurityEventCondition(
                DetectedSecurityEventCondition.newBuilder()
                    .addAllEventTypes(
                        Collections.singletonList(
                            SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                    .addAllSeverities(
                        Collections.singletonList(
                            SecurityEventSeverity.SECURITY_EVENT_SEVERITY_HIGH))
                    .build())
            .setScope(
                EventConditionScope.newBuilder()
                    .setEnvironmentScope(
                        EventConditionScope.EnvironmentScope.newBuilder()
                            .addAllEnvironmentNames(List.of("env1", "env2"))
                            .build())
                    .build())
            .build();

    // update with envScope list and no env field set
    eventConditionsStub
        .updateEventCondition(
            UpdateEventConditionRequest.newBuilder()
                .setId(actual1.getId())
                .setEventConditionMutableData(data2)
                .build())
        .getEventCondition();

    EventCondition actual2 =
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.newBuilder().build())
            .getEventConditionsList()
            .get(0);

    // if both env field and envScope list is present then only the list should be returned
    EventCondition expected =
        EventCondition.newBuilder()
            .setId(actual2.getId())
            .setEventConditionMutableData(
                EventConditionMutableData.newBuilder(data2)
                    .clearEnvironment()
                    .setScope(
                        EventConditionScope.newBuilder(data1.getScope())
                            .setEnvironmentScope(
                                EventConditionScope.EnvironmentScope.newBuilder()
                                    .addAllEnvironmentNames(List.of("env1", "env2"))
                                    .build())
                            .build())
                    .build())
            .build();

    assertEquals(expected, actual2);
    assertFalse(actual2.getEventConditionMutableData().hasEnvironment());
  }

  @Test
  public void when_EnvIsNotEmpty_And_EnvScopeListINotEmpty_ThenExpectMergedEnvScopeListOnWrite() {
    EventConditionMutableData data =
        EventConditionMutableData.newBuilder()
            .setDetectedSecurityEventCondition(
                DetectedSecurityEventCondition.newBuilder()
                    .addAllEventTypes(
                        Collections.singletonList(
                            SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                    .addAllSeverities(
                        Collections.singletonList(
                            SecurityEventSeverity.SECURITY_EVENT_SEVERITY_HIGH))
                    .build())
            .setScope(
                EventConditionScope.newBuilder()
                    .setEnvironmentScope(
                        EventConditionScope.EnvironmentScope.newBuilder()
                            .addAllEnvironmentNames(List.of("env1", "env2"))
                            .build())
                    .build())
            .setEnvironment("env3")
            .build();

    EventCondition actual =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder().setEventConditionMutableData(data).build())
            .getEventCondition();

    EventCondition expected =
        EventCondition.newBuilder()
            .setId(actual.getId())
            .setEventConditionMutableData(
                EventConditionMutableData.newBuilder(data)
                    .setScope(
                        EventConditionScope.newBuilder(data.getScope())
                            .setEnvironmentScope(
                                EventConditionScope.EnvironmentScope.newBuilder()
                                    .addAllEnvironmentNames(List.of("env1", "env2", "env3"))
                                    .build())
                            .build())
                    .build())
            .build();

    assertEquals(expected, actual);
  }

  @Test
  public void when_EnvIsEmpty_And_EnvScopeListINotEmpty_ThenExpectTheSameDuringReads() {
    EventConditionMutableData data =
        EventConditionMutableData.newBuilder()
            .setDetectedSecurityEventCondition(
                DetectedSecurityEventCondition.newBuilder()
                    .addAllEventTypes(
                        Collections.singletonList(
                            SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                    .addAllSeverities(
                        Collections.singletonList(
                            SecurityEventSeverity.SECURITY_EVENT_SEVERITY_HIGH))
                    .build())
            .setScope(
                EventConditionScope.newBuilder()
                    .setEnvironmentScope(
                        EventConditionScope.EnvironmentScope.newBuilder()
                            .addAllEnvironmentNames(List.of("env1", "env2"))
                            .build())
                    .build())
            .build();

    EventCondition actual =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder().setEventConditionMutableData(data).build())
            .getEventCondition();
    EventCondition expected =
        EventCondition.newBuilder()
            .setId(actual.getId())
            .setEventConditionMutableData(data)
            .build();
    assertEquals(expected, actual);
  }

  @Test
  public void
      when_EnvIsNotEmpty_And_EnvScopeListIEmpty_ThenExpectBothTheFieldAndListToHaveTheSameEnv() {
    EventConditionMutableData data =
        EventConditionMutableData.newBuilder()
            .setDetectedSecurityEventCondition(
                DetectedSecurityEventCondition.newBuilder()
                    .addAllEventTypes(
                        Collections.singletonList(
                            SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                    .addAllSeverities(
                        Collections.singletonList(
                            SecurityEventSeverity.SECURITY_EVENT_SEVERITY_HIGH))
                    .build())
            .setEnvironment("env1")
            .build();

    eventConditionsStub
        .createEventCondition(
            CreateEventConditionRequest.newBuilder()
                .setEventConditionMutableData(EventConditionMutableData.newBuilder(data).build())
                .build())
        .getEventCondition();

    EventCondition actual =
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionsList()
            .get(0);

    assertEquals(
        List.of("env1"),
        actual
            .getEventConditionMutableData()
            .getScope()
            .getEnvironmentScope()
            .getEnvironmentNamesList());
  }

  private EventConditionMutableData getEventConditionMutableData() {
    return EventConditionMutableData.newBuilder()
        .setDetectedSecurityEventCondition(
            DetectedSecurityEventCondition.newBuilder()
                .addAllEventTypes(
                    Collections.singletonList(
                        SecurityEventType.SECURITY_EVENT_TYPE_SCANNER_DETECTED))
                .addAllSeverities(
                    Collections.singletonList(SecurityEventSeverity.SECURITY_EVENT_SEVERITY_HIGH))
                .build())
        .setScope(
            EventConditionScope.newBuilder()
                .setEnvironmentScope(
                    EventConditionScope.EnvironmentScope.newBuilder()
                        .addAllEnvironmentNames(List.of("dev1", "dev2"))
                        .build())
                .build())
        .build();
  }
}
