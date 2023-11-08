package ai.traceable.span.processing.config.service.spaningestionrules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DataLocation;
import ai.traceable.span.processing.config.service.v1.IngestionStage;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleData;
import ai.traceable.span.processing.config.service.v1.Predicate;
import ai.traceable.span.processing.config.service.v1.RetentionAction;
import ai.traceable.span.processing.config.service.v1.RetentionAction.RetainAction;
import ai.traceable.span.processing.config.service.v1.StringPredicate;
import ai.traceable.span.processing.config.service.v1.StringPredicate.Operator;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SpanIngestionRuleBuilderTest {

  @Mock UuidGenerator uuidGenerator;
  SpanIngestionRuleBuilder ruleBuilder;

  @BeforeEach
  void setup() {
    ruleBuilder = new SpanIngestionRuleBuilder(uuidGenerator);
  }

  @Test
  void createValidPersistedKeyValueRetentionRule() {
    CreateSpanIngestionRuleRequest request =
        CreateSpanIngestionRuleRequest.newBuilder()
            .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
            .setRequestHeaderRule(
                KeyValueRetentionRuleData.newBuilder()
                    .setAction(
                        RetentionAction.newBuilder().setRetain(RetainAction.getDefaultInstance()))
                    .setPredicate(
                        Predicate.newBuilder()
                            .setTargetKeyPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(Operator.OPERATOR_EQUALS)
                                    .setValue("test-header")))
                    .setExpiration(Timestamp.newBuilder().setSeconds(1913807880)))
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn("id-1");
    PersistedKeyValueRetentionRule actual = this.ruleBuilder.generateNewRuleWithoutRank(request);
    PersistedKeyValueRetentionRule expected =
        PersistedKeyValueRetentionRule.newBuilder()
            .setId("id-1")
            .setIngestionStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
            .setData(
                KeyValueRetentionRuleData.newBuilder()
                    .setAction(
                        RetentionAction.newBuilder()
                            .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                    .setPredicate(
                        Predicate.newBuilder()
                            .setTargetKeyPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                    .setValue("test-header")))
                    .setExpiration(Timestamp.newBuilder().setSeconds(1913807880)))
            .setLocation(DataLocation.DATA_LOCATION_REQUEST_HEADER)
            .build();

    assertEquals(expected, actual);
  }
}
