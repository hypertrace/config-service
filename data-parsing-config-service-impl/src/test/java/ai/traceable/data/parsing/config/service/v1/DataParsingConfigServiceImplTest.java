package ai.traceable.data.parsing.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.data.parsing.config.service.v1.store.DataParsingRuleStore;
import ai.traceable.data.parsing.config.service.v1.utils.DataParsingConfigGenerator;
import ai.traceable.data.parsing.config.service.v1.utils.DataParsingConfigRankCalculator;
import ai.traceable.data.parsing.config.service.v1.validation.DataParsingConfigRequestValidator;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataParsingConfigServiceImplTest {

  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant");
  private DataParsingRuleStore ruleStore;
  private DataParsingConfigGenerator generator;
  private DataParsingConfigRankCalculator rankCalculator;
  private DataParsingConfigServiceImpl service;

  private static final DataParsingRule RULE =
      DataParsingRule.newBuilder()
          .setEnabled(true)
          .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
          .build();

  private static final DataParsingConfig CONFIG =
      DataParsingConfig.newBuilder().setId("id-1").setDataParsingRule(RULE).build();

  @BeforeEach
  void setup() {
    ruleStore = mock(DataParsingRuleStore.class);
    generator = mock(DataParsingConfigGenerator.class);
    rankCalculator = mock(DataParsingConfigRankCalculator.class);
    service =
        new DataParsingConfigServiceImpl(
            mock(DataParsingConfigRequestValidator.class),
            ruleStore,
            generator,
            rankCalculator,
            mock(ObjectDiffer.class));
  }

  @Test
  void testGetDataParsingRules() {
    StreamObserver<GetDataParsingRulesResponse> observer = mock(StreamObserver.class);
    when(ruleStore.getDataParsingConfigs(any(), any())).thenReturn(List.of(CONFIG));

    REQUEST_CONTEXT.call(
        () -> {
          service.getDataParsingRules(GetDataParsingRulesRequest.getDefaultInstance(), observer);
          return null;
        });

    verify(observer)
        .onNext(GetDataParsingRulesResponse.newBuilder().addDataParsingConfigs(CONFIG).build());
    verify(observer).onCompleted();
  }

  @Test
  void testCreateDataParsingRule() {
    StreamObserver<CreateDataParsingRuleResponse> observer = mock(StreamObserver.class);
    CreateDataParsingRuleRequest request =
        CreateDataParsingRuleRequest.newBuilder().setDataParsingRule(RULE).build();

    when(generator.generateNewConfig(request)).thenReturn(CONFIG);
    when(ruleStore.getAllData(any())).thenReturn(List.of());
    when(rankCalculator.rankAndMergeNewObject(eq(CONFIG), any())).thenReturn(List.of(CONFIG));

    REQUEST_CONTEXT.call(
        () -> {
          service.createDataParsingRule(request, observer);
          return null;
        });

    verify(observer)
        .onNext(CreateDataParsingRuleResponse.newBuilder().setDataParsingConfig(CONFIG).build());
    verify(observer).onCompleted();
  }

  @Test
  void testUpdateDataParsingRule() {
    StreamObserver<UpdateDataParsingRuleResponse> observer = mock(StreamObserver.class);
    UpdateDataParsingRuleRequest request =
        UpdateDataParsingRuleRequest.newBuilder().setId("id-1").setDataParsingRule(RULE).build();

    when(ruleStore.getData(any(), eq("id-1"))).thenReturn(Optional.of(CONFIG));

    REQUEST_CONTEXT.call(
        () -> {
          service.updateDataParsingRule(request, observer);
          return null;
        });

    verify(observer).onNext(any(UpdateDataParsingRuleResponse.class));
    verify(observer).onCompleted();
  }

  @Test
  void testDeleteDataParsingRule() {
    StreamObserver<DeleteDataParsingRuleResponse> observer = mock(StreamObserver.class);
    DeleteDataParsingRuleRequest request =
        DeleteDataParsingRuleRequest.newBuilder().setId("id-1").build();

    REQUEST_CONTEXT.call(
        () -> {
          service.deleteDataParsingRule(request, observer);
          return null;
        });

    verify(ruleStore).deleteObject(any(), eq("id-1"));
    verify(observer).onNext(DeleteDataParsingRuleResponse.getDefaultInstance());
    verify(observer).onCompleted();
  }
}
