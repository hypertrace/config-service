package ai.traceable.alerting.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import ai.traceable.alerting.config.service.v2.CreateEventConditionRequest;
import ai.traceable.alerting.config.service.v2.DeleteEventConditionRequest;
import ai.traceable.alerting.config.service.v2.DetectedSecurityEventCondition;
import ai.traceable.alerting.config.service.v2.EventCondition;
import ai.traceable.alerting.config.service.v2.EventConditionConfigServiceGrpc;
import ai.traceable.alerting.config.service.v2.EventConditionConfigServiceGrpc.EventConditionConfigServiceBlockingStub;
import ai.traceable.alerting.config.service.v2.EventConditionMutableData;
import ai.traceable.alerting.config.service.v2.GetAllEventConditionsRequest;
import ai.traceable.alerting.config.service.v2.SecurityEventSeverity;
import ai.traceable.alerting.config.service.v2.SecurityEventType;
import ai.traceable.alerting.config.service.v2.UpdateEventConditionRequest;
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

    EventCondition eventCondition1ToUpdate =
        eventCondition1.toBuilder()
            .setEventConditionMutableData(
                EventConditionMutableData.newBuilder()
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

    EventCondition updatedEventCondition1 =
        eventConditionsStub
            .updateEventCondition(
                UpdateEventConditionRequest.newBuilder()
                    .setId(eventCondition1ToUpdate.getId())
                    .setEventConditionMutableData(
                        eventCondition1ToUpdate.getEventConditionMutableData())
                    .build())
            .getEventCondition();

    assertEquals(eventCondition1ToUpdate, updatedEventCondition1);

    assertIterableEquals(
        List.of(eventCondition2, updatedEventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionsList());

    eventConditionsStub.deleteEventCondition(
        DeleteEventConditionRequest.newBuilder()
            .setEventConditionId(eventCondition2.getId())
            .build());

    assertIterableEquals(
        List.of(updatedEventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionsList());
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
        .setEnvironment("dev")
        .build();
  }
}
