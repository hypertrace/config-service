package ai.traceable.ratelimiting.config.service.v2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceImpl;
import ai.traceable.ratelimiting.service.v2.rules.RulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RulesValidator;
import ai.traceable.ratelimiting.service.v2.rules.converter.RateLimitingEdgeDecisionConverter;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RateLimitingConfigServiceImplTest {
  private static final String TENANT_ID = "default tenant";

  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private ActivityEventProducer activityEventProducer;
  private RateLimitingConfigServiceImpl configService;
  private FeatureCachingClient featureCachingClient;

  @BeforeEach
  void setUp() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    activityEventProducer = mock(ActivityEventProducer.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    // existingRules = Collections.emptyList();
    RateLimitingConfigServiceConfig config = mock(RateLimitingConfigServiceConfig.class);
    RateLimitingEdgeDecisionConverter translator = mock(RateLimitingEdgeDecisionConverter.class);
    when(config.shouldPublishActivityEvents()).thenReturn(true);
    configService =
        new RateLimitingConfigServiceImpl(
            rulesValidator,
            rulesManager,
            activityEventProducer,
            config,
            translator,
            featureCachingClient);
  }

  @Test
  void testCreateRateLimitingRule() {
    StreamObserver<CreateRateLimitingRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.createRateLimitingRule(
                CreateRateLimitingRuleRequest.getDefaultInstance(), responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (CreateRateLimitingRuleRequest) any(), any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    RateLimitingRule rule = buildRateLimitingRule("id", "rule", Category.CATEGORY_RATE_LIMITING);
    reset(responseObserver);
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(any(), (CreateRateLimitingRuleRequest) any(), any());
    when(rulesManager.createRateLimitingRule(any(), any())).thenReturn(rule);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(CreateRateLimitingRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
    verify(activityEventProducer, times(1))
        .publishSecurityConfigurationChangeEvent(
            any(RequestContext.class),
            eq(
                SecurityConfigurationChange.newBuilder()
                    .setRuleId(rule.getId())
                    .setRuleName(rule.getData().getName())
                    .setSecurityConfigurationType(SecurityConfigurationType.RATE_LIMITING_RULE)
                    .setSecurityConfigurationAction(SecurityConfigurationAction.ADD)
                    .build()));
  }

  @Test
  void testGetRateLimitingRules() {
    StreamObserver<GetRateLimitingRulesResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.getDefaultInstance(), responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1)).onNext(GetRateLimitingRulesResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();

    List<RateLimitingRule> rules =
        List.of(
            buildRateLimitingRule("id1", "rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id2", "rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id3", "rule3", Category.CATEGORY_DATA_EXFILTRATION));
    reset(responseObserver);
    when(rulesManager.getRateLimitingRules(any(), any())).thenReturn(rules);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(GetRateLimitingRulesResponse.newBuilder().addAllRules(rules).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateRateLimitingRule() {
    StreamObserver<UpdateRateLimitingRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.updateRateLimitingRule(
                UpdateRateLimitingRuleRequest.getDefaultInstance(), responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (UpdateRateLimitingRuleRequest) any(), any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    RateLimitingRule rule = buildRateLimitingRule("id", "rule", Category.CATEGORY_RATE_LIMITING);
    reset(responseObserver);
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(any(), (UpdateRateLimitingRuleRequest) any(), any());
    when(rulesManager.updateRateLimitingRule(any(), any(), any())).thenReturn(rule);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(UpdateRateLimitingRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
    verify(activityEventProducer, times(1))
        .publishSecurityConfigurationChangeEvent(
            any(RequestContext.class),
            eq(
                SecurityConfigurationChange.newBuilder()
                    .setRuleId(rule.getId())
                    .setRuleName(rule.getData().getName())
                    .setSecurityConfigurationType(SecurityConfigurationType.RATE_LIMITING_RULE)
                    .setSecurityConfigurationAction(SecurityConfigurationAction.UPDATE)
                    .build()));
  }

  @Test
  void testDeleteRateLimitingRule() {
    StreamObserver<DeleteRateLimitingRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.deleteRateLimitingRule(
                DeleteRateLimitingRuleRequest.getDefaultInstance(), responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (DeleteRateLimitingRuleRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    RateLimitingRule rule = buildRateLimitingRule("id", "rule", Category.CATEGORY_RATE_LIMITING);
    reset(responseObserver);
    doNothing().when(rulesValidator).validateOrThrow(any(), (DeleteRateLimitingRuleRequest) any());
    when(rulesManager.deleteRateLimitingRule(any(), any())).thenReturn(Optional.of(rule));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1)).onNext(DeleteRateLimitingRuleResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
    verify(activityEventProducer, times(1))
        .publishSecurityConfigurationChangeEvent(
            any(RequestContext.class),
            eq(
                SecurityConfigurationChange.newBuilder()
                    .setRuleId(rule.getId())
                    .setRuleName(rule.getData().getName())
                    .setSecurityConfigurationType(SecurityConfigurationType.RATE_LIMITING_RULE)
                    .setSecurityConfigurationAction(SecurityConfigurationAction.REMOVE)
                    .build()));
  }

  private RateLimitingRule buildRateLimitingRule(String id, String name, Category category) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(buildRateLimitingRuleData(name, category))
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(String name, Category category) {
    return RateLimitingRuleData.newBuilder().setName(name).setCategory(category).build();
  }
}
