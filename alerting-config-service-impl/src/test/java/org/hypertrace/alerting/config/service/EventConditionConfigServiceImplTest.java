package org.hypertrace.alerting.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;
import org.hypertrace.alerting.config.service.v1.Attribute;
import org.hypertrace.alerting.config.service.v1.BaselineThresholdCondition;
import org.hypertrace.alerting.config.service.v1.CreateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.DeleteEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.EventCondition;
import org.hypertrace.alerting.config.service.v1.EventConditionConfigServiceGrpc;
import org.hypertrace.alerting.config.service.v1.EventConditionConfigServiceGrpc.EventConditionConfigServiceBlockingStub;
import org.hypertrace.alerting.config.service.v1.EventConditionMutableData;
import org.hypertrace.alerting.config.service.v1.Filter;
import org.hypertrace.alerting.config.service.v1.GetAllEventConditionsRequest;
import org.hypertrace.alerting.config.service.v1.LeafFilter;
import org.hypertrace.alerting.config.service.v1.LhsExpression;
import org.hypertrace.alerting.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alerting.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alerting.config.service.v1.MetricSelection;
import org.hypertrace.alerting.config.service.v1.NewEventCondition;
import org.hypertrace.alerting.config.service.v1.RhsExpression;
import org.hypertrace.alerting.config.service.v1.UpdateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.ValueOperator;
import org.hypertrace.alerting.config.service.v1.ViolationCondition;
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

    NewEventCondition newEventCondition1 = getNewEventCondition();
    EventCondition eventCondition1 =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder()
                    .setNewEventCondition(newEventCondition1)
                    .build())
            .getEventCondition();
    assertEquals(
        newEventCondition1.getEventConditionData().getConditionCase().name(),
        eventCondition1.getEventConditionData().getConditionCase().name());
    assertFalse(eventCondition1.getId().isEmpty());

    EventCondition eventCondition2 =
        eventConditionsStub
            .createEventCondition(
                CreateEventConditionRequest.newBuilder()
                    .setNewEventCondition(newEventCondition1)
                    .build())
            .getEventCondition();
    assertIterableEquals(
        List.of(eventCondition2, eventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionList());

    EventCondition eventCondition1ToUpdate =
        eventCondition1.toBuilder()
            .setEventConditionData(
                EventConditionMutableData.newBuilder()
                    .setMetricAnomalyEventCondition(getMetricAnomalyEventCondition("PT5M"))
                    .build())
            .build();

    EventCondition updatedEventCondition1 =
        eventConditionsStub
            .updateEventCondition(
                UpdateEventConditionRequest.newBuilder()
                    .setEventCondition(eventCondition1ToUpdate)
                    .build())
            .getEventCondition();

    assertEquals(eventCondition1ToUpdate, updatedEventCondition1);

    assertIterableEquals(
        List.of(eventCondition2, updatedEventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionList());

    eventConditionsStub.deleteEventCondition(
        DeleteEventConditionRequest.newBuilder()
            .setEventConditionId(eventCondition2.getId())
            .build());

    assertIterableEquals(
        List.of(updatedEventCondition1),
        eventConditionsStub
            .getAllEventConditions(GetAllEventConditionsRequest.getDefaultInstance())
            .getEventConditionList());
  }

  private NewEventCondition getNewEventCondition() {
    return NewEventCondition.newBuilder()
        .setEventConditionData(
            EventConditionMutableData.newBuilder()
                .setMetricAnomalyEventCondition(getMetricAnomalyEventCondition("PT1M"))
                .build())
        .build();
  }

  private MetricAnomalyEventCondition getMetricAnomalyEventCondition(
      String evaluationWindowDuration) {
    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("name").setScope("SERVICE").build())
            .build();
    RhsExpression rhsExpression = RhsExpression.newBuilder().setStringValue("frontend").build();
    LeafFilter leafFilter =
        LeafFilter.newBuilder()
            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
            .setLhsExpression(lhsExpression)
            .setRhsExpression(rhsExpression)
            .build();
    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT1M")
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVGRATE)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("errorCount").setScope("SERVICE").build())
            .build();

    MetricAnomalyEventCondition.Builder metricAnomalyEventConditionBuilder =
        MetricAnomalyEventCondition.newBuilder();
    metricAnomalyEventConditionBuilder.setEvaluationWindowDuration(evaluationWindowDuration);
    metricAnomalyEventConditionBuilder.setMetricSelection(metricSelection);
    metricAnomalyEventConditionBuilder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setBaselineThresholdCondition(
                BaselineThresholdCondition.newBuilder().setBaselineDuration("PT5M").build())
            .build());
    return metricAnomalyEventConditionBuilder.build();
  }
}
