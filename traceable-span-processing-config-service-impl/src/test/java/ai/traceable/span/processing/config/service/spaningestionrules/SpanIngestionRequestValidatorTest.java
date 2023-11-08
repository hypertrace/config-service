package ai.traceable.span.processing.config.service.spaningestionrules;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.IngestionStage;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleData;
import ai.traceable.span.processing.config.service.v1.Predicate;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.RetentionAction;
import ai.traceable.span.processing.config.service.v1.StringPredicate;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SpanIngestionRequestValidatorTest {

  private static final RequestContext VALID_REQUEST_CONTEXT = RequestContext.forTenantId("test-id");

  private static final GetSpanIngestionConfigRequest VALID_GET_CONFIG_REQUEST =
      GetSpanIngestionConfigRequest.newBuilder()
          .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
          .build();

  private static final KeyValueRetentionRuleData VALID_KEY_VALUE_RETENTION_RULE_DATA =
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
          .setExpiration(Timestamp.newBuilder().setSeconds(1913807880))
          .build();

  private static final CreateSpanIngestionRuleRequest VALID_CREATE_HEADER_RULE_REQUEST =
      CreateSpanIngestionRuleRequest.newBuilder()
          .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
          .setRequestHeaderRule(VALID_KEY_VALUE_RETENTION_RULE_DATA)
          .build();

  private static final DeleteSpanIngestionRuleRequest VALID_DELETE_RULE_REQUEST =
      DeleteSpanIngestionRuleRequest.newBuilder().setId("test-id").build();

  private static final UpdateSpanIngestionRuleRequest VALID_UPDATE_RULE_REQUEST =
      UpdateSpanIngestionRuleRequest.newBuilder()
          .setId("test-id")
          .setKeyValueRetentionRuleData(VALID_KEY_VALUE_RETENTION_RULE_DATA)
          .build();

  private static final RankSpanIngestionRuleRequest VALID_RANK_RULE_REQUEST =
      RankSpanIngestionRuleRequest.newBuilder()
          .setIdToUpdate("id-2")
          .setPrecedingRuleId("id-1")
          .build();

  SpanIngestionRequestValidator validator = new SpanIngestionRequestValidator();

  @Test
  void acceptsValidRequests() {
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_GET_CONFIG_REQUEST);
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_CREATE_HEADER_RULE_REQUEST);
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_DELETE_RULE_REQUEST);
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_UPDATE_RULE_REQUEST);
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_RANK_RULE_REQUEST);
  }

  @Test
  void rejectsMissingRequestContext() {
    this.assertInvalidArg(
        () -> this.validator.validateOrThrow(new RequestContext(), VALID_GET_CONFIG_REQUEST));
    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(new RequestContext(), VALID_CREATE_HEADER_RULE_REQUEST));
    this.assertInvalidArg(
        () -> this.validator.validateOrThrow(new RequestContext(), VALID_DELETE_RULE_REQUEST));
    this.assertInvalidArg(
        () -> this.validator.validateOrThrow(new RequestContext(), VALID_UPDATE_RULE_REQUEST));
    this.assertInvalidArg(
        () -> this.validator.validateOrThrow(new RequestContext(), VALID_RANK_RULE_REQUEST));
  }

  @Test
  void rejectsBadGetConfigRequest() {
    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_GET_CONFIG_REQUEST.toBuilder()
                    .clearStage()
                    .setStage(IngestionStage.INGESTION_STAGE_UNSPECIFIED)
                    .build()));
  }

  @Test
  void rejectsBadCreateRequest() {
    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_CREATE_HEADER_RULE_REQUEST.toBuilder()
                    .clearRequestHeaderRule()
                    .setRequestHeaderRule(
                        VALID_KEY_VALUE_RETENTION_RULE_DATA.toBuilder().clearAction().build())
                    .build()));

    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_CREATE_HEADER_RULE_REQUEST.toBuilder()
                    .clearRequestHeaderRule()
                    .setRequestHeaderRule(
                        VALID_KEY_VALUE_RETENTION_RULE_DATA.toBuilder()
                            .clearPredicate()
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setValue("header-1") // operator missing
                                        ))
                            .build())
                    .build()));

    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_CREATE_HEADER_RULE_REQUEST.toBuilder()
                    .clearRequestHeaderRule()
                    .setRequestHeaderRule(
                        VALID_KEY_VALUE_RETENTION_RULE_DATA.toBuilder()
                            .clearPredicate()
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                StringPredicate.Operator
                                                    .OPERATOR_EQUALS) // value missing
                                        ))
                            .build())
                    .build()));
  }

  @Test
  void rejectsBadUpdateRequest() {
    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT, VALID_UPDATE_RULE_REQUEST.toBuilder().clearId().build()));

    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT, VALID_UPDATE_RULE_REQUEST.toBuilder().clearRule().build()));

    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_UPDATE_RULE_REQUEST.toBuilder()
                    .clearRule()
                    .setKeyValueRetentionRuleData(
                        VALID_KEY_VALUE_RETENTION_RULE_DATA.toBuilder().clearAction().build())
                    .build())); // missing action
  }

  @Test
  void rejectsBadRankRuleRequest() {
    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_RANK_RULE_REQUEST.toBuilder().clearIdToUpdate().build()));

    this.assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_RANK_RULE_REQUEST.toBuilder()
                    .setPrecedingRuleId(VALID_RANK_RULE_REQUEST.getIdToUpdate())
                    .build()));
  }

  void assertInvalidArg(Executable executable) {
    assertEquals(
        Status.Code.INVALID_ARGUMENT,
        Assertions.assertThrows(StatusRuntimeException.class, executable).getStatus().getCode());
  }
}
