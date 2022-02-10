package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_OBFUSCATE_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_DESCRIPTION;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.sensitivedata.config.service.v1.ComplexData;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RedactionRulesDaoTest {
  MockGenericConfigService mockGenericConfigService;
  RedactionRulesDao redactionRulesDao;
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-test");

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    mockGenericConfigService.addService(new MockSensitiveDataConfigService()).start();

    redactionRulesDao = new RedactionRulesDao(sensitiveDataConfigServiceBlockingStub);
  }

  @AfterEach
  void tearDown() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void dataTypesFromRedactionRulesTest() {
    DataType expectedDataType1 =
        DataType.newBuilder()
            .setId("id-2")
            .setRule(DataTypeRule.newBuilder().setName("rule-2").setDescription("description-2"))
            .build();
    DataType expectedDataType2 =
        DataType.newBuilder()
            .setId("id-3")
            .setRule(DataTypeRule.newBuilder().setName("rule-3").setDescription("description-3"))
            .build();
    List<DataType> actualDataTypes =
        redactionRulesDao.getAllDataTypesFromRedactionRules(REQUEST_CONTEXT);
    assertEquals(2, actualDataTypes.size());
    assertEquals(expectedDataType1, actualDataTypes.get(0));
    assertEquals(expectedDataType2, actualDataTypes.get(1));
  }

  @Test
  void getDataSetFromRedactionRulesTest() {
    DataSetInfo expectedDataSetInfo =
        DataSetInfo.newBuilder()
            .setName(LEGACY_RAW_DATA_SET_NAME)
            .setDescription(LEGACY_RAW_DATA_SET_DESCRIPTION)
            .setEnabled(true)
            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
            .addDataTypeIds("id-3")
            .build();
    Optional<DataSet> actualDataSet =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_RAW_DATA_SET_ID);
    assertFalse(actualDataSet.isEmpty());
    assertEquals(expectedDataSetInfo, actualDataSet.get().getInfo());
  }

  @Test
  void getDataSetsFromRedactionRulesTest() {
    List<DataSet> actualDataSets = redactionRulesDao.getDataSetsFromRedactionRules(REQUEST_CONTEXT);
    assertEquals(2, actualDataSets.size());
    assertEquals(LEGACY_OBFUSCATE_DATA_SET_ID, actualDataSets.get(0).getId());
    assertEquals("id-2", actualDataSets.get(0).getInfo().getDataTypeIds(0));
    assertEquals(LEGACY_RAW_DATA_SET_ID, actualDataSets.get(1).getId());
    assertEquals("id-3", actualDataSets.get(1).getInfo().getDataTypeIds(0));
  }

  class MockSensitiveDataConfigService
      extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

    @Override
    public void getAllRedactionRules(
        GetAllRedactionRulesRequest request,
        StreamObserver<GetAllRedactionRulesResponse> responseObserver) {
      GetAllRedactionRulesResponse.Builder responseBuilder =
          GetAllRedactionRulesResponse.newBuilder();
      RedactionRule rule1 =
          RedactionRule.newBuilder()
              .setId("id-1")
              .setName("rule-1")
              .setDescription("description-1")
              .setCategory("category-1")
              .setMatchType(MatchType.MATCH_TYPE_KEY)
              .setComplexData(ComplexData.getDefaultInstance())
              .setRegex("regex*")
              .setSessionIdentifier(true)
              .setFqn(false)
              .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
              .build();
      RedactionRule rule2 =
          RedactionRule.newBuilder()
              .setId("id-2")
              .setName("rule-2")
              .setDescription("description-2")
              .setCategory("category-2")
              .setMatchType(MatchType.MATCH_TYPE_KEY)
              .setComplexData(ComplexData.getDefaultInstance())
              .setRegex("reg*ex")
              .setSessionIdentifier(false)
              .setFqn(true)
              .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
              .build();
      RedactionRule rule3 =
          RedactionRule.newBuilder()
              .setId("id-3")
              .setName("rule-3")
              .setDescription("description-3")
              .setCategory("category-3")
              .setMatchType(MatchType.MATCH_TYPE_KEY)
              .setComplexData(ComplexData.getDefaultInstance())
              .setRegex("regex")
              .setSessionIdentifier(false)
              .setFqn(false)
              .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
              .build();
      responseBuilder.addAllRedactionRules(List.of(rule1, rule2, rule3));
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }
}
