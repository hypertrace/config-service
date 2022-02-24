package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_DESCRIPTION;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_NAME;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_OBFUSCATE_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_DESCRIPTION;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_NAME;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_SENSITIVE_HEADERS_DATA_SET_DESCRIPTION;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_SENSITIVE_HEADERS_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_SENSITIVE_HEADERS_DATA_SET_NAME;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_SENSITIVE_HEADERS_DATA_TYPE;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.sensitivedata.config.service.v1.ComplexData;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeResponse;
import io.grpc.stub.StreamObserver;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
    DataType expectedDataType3 =
        DataType.newBuilder()
            .setId("id-4")
            .setRule(DataTypeRule.newBuilder().setName("rule-4").setDescription("description-4"))
            .build();
    List<DataType> actualDataTypes =
        redactionRulesDao.getAllDataTypesFromRedactionRules(REQUEST_CONTEXT);
    assertEquals(5, actualDataTypes.size());
    assertEquals(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE, actualDataTypes.get(0));
    assertEquals(LEGACY_SENSITIVE_HEADERS_DATA_TYPE, actualDataTypes.get(1));
    assertEquals(expectedDataType1, actualDataTypes.get(2));
    assertEquals(expectedDataType2, actualDataTypes.get(3));
    assertEquals(expectedDataType3, actualDataTypes.get(4));
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
            .addDataTypeIds("id-4")
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
    assertEquals(4, actualDataSets.size());
    assertEquals(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, actualDataSets.get(0).getId());
    assertEquals(
        LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID,
        actualDataSets.get(0).getInfo().getDataTypeIds(0));
    assertEquals(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, actualDataSets.get(1).getId());
    assertEquals(
        LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID, actualDataSets.get(1).getInfo().getDataTypeIds(0));
    assertEquals(LEGACY_OBFUSCATE_DATA_SET_ID, actualDataSets.get(2).getId());
    assertEquals("id-2", actualDataSets.get(2).getInfo().getDataTypeIds(0));
    assertEquals(LEGACY_RAW_DATA_SET_ID, actualDataSets.get(3).getId());
    assertEquals("id-3", actualDataSets.get(3).getInfo().getDataTypeIds(0));
  }

  @Test
  void updateDataSetTest() {
    DataSetInfo updatedDataSetInfo =
        DataSetInfo.newBuilder()
            .setName(LEGACY_RAW_DATA_SET_NAME)
            .setDescription(LEGACY_RAW_DATA_SET_DESCRIPTION)
            .setEnabled(true)
            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
            .addDataTypeIds("id-3")
            .build();
    redactionRulesDao.updateDataSet(REQUEST_CONTEXT, LEGACY_RAW_DATA_SET_ID, updatedDataSetInfo);
    Optional<DataSet> dataSetOptional =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_RAW_DATA_SET_ID);
    assertTrue(dataSetOptional.isPresent());
    assertEquals(updatedDataSetInfo, dataSetOptional.get().getInfo());
  }

  @Test
  void deleteAutomaticSecretRedactionDataSetTest() {
    redactionRulesDao.deleteDataSet(REQUEST_CONTEXT, LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID);
    Optional<DataSet> dataSetOptional =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID);
    assertTrue(dataSetOptional.isEmpty());
  }

  @Test
  void updateAutomaticSecretRedactionDataSetTest() {
    DataSetInfo updatedDataSetInfo =
        DataSetInfo.newBuilder()
            .setName(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_NAME)
            .setDescription(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_DESCRIPTION)
            .setEnabled(true)
            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
            .build();
    redactionRulesDao.updateDataSet(
        REQUEST_CONTEXT, LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, updatedDataSetInfo);
    Optional<DataSet> dataSetOptional =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID);
    assertTrue(dataSetOptional.isEmpty());
  }

  @Test
  void deleteSensitiveHeadersDataSetTest() {
    redactionRulesDao.deleteDataSet(REQUEST_CONTEXT, LEGACY_SENSITIVE_HEADERS_DATA_SET_ID);
    Optional<DataSet> dataSetOptional =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_SENSITIVE_HEADERS_DATA_SET_ID);
    assertTrue(dataSetOptional.isEmpty());
  }

  @Test
  void updateSensitiveHeadersDataSetTest() {
    DataSetInfo updatedDataSetInfo =
        DataSetInfo.newBuilder()
            .setName(LEGACY_SENSITIVE_HEADERS_DATA_SET_NAME)
            .setDescription(LEGACY_SENSITIVE_HEADERS_DATA_SET_DESCRIPTION)
            .setEnabled(true)
            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
            .build();
    redactionRulesDao.updateDataSet(
        REQUEST_CONTEXT, LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, updatedDataSetInfo);
    Optional<DataSet> dataSetOptional =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_SENSITIVE_HEADERS_DATA_SET_ID);
    assertTrue(dataSetOptional.isEmpty());
  }

  @Test
  void deleteDataSetTest() {
    redactionRulesDao.deleteDataSet(REQUEST_CONTEXT, LEGACY_RAW_DATA_SET_ID);
    Optional<DataSet> dataSetOptional =
        redactionRulesDao.getDataSetWithIdFromRedactionRules(
            REQUEST_CONTEXT, LEGACY_RAW_DATA_SET_ID);
    assertTrue(dataSetOptional.isEmpty());
  }

  class MockSensitiveDataConfigService
      extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

    private final Map<String, RedactionRule> redactionRulesMap;
    private boolean automaticSecretRedaction = true;
    private RedactionStrategy headerRedactionStrategy = REDACTION_STRATEGY_REDACT;

    public MockSensitiveDataConfigService() {
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
              .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
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
      RedactionRule rule4 =
          RedactionRule.newBuilder()
              .setId("id-4")
              .setName("rule-4")
              .setDescription("description-4")
              .setCategory("category-4")
              .setMatchType(MatchType.MATCH_TYPE_KEY)
              .setComplexData(ComplexData.getDefaultInstance())
              .setRegex("regex")
              .setSessionIdentifier(false)
              .setFqn(false)
              .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
              .build();
      redactionRulesMap =
          Stream.of(rule1, rule2, rule3, rule4)
              .collect(
                  Collectors.toMap(
                      RedactionRule::getId, Function.identity(), (x, y) -> y, LinkedHashMap::new));
    }

    @Override
    public void getAllRedactionRules(
        GetAllRedactionRulesRequest request,
        StreamObserver<GetAllRedactionRulesResponse> responseObserver) {
      GetAllRedactionRulesResponse.Builder responseBuilder =
          GetAllRedactionRulesResponse.newBuilder();

      responseBuilder.addAllRedactionRules(redactionRulesMap.values());
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteRedactionRule(
        DeleteRedactionRuleRequest request,
        StreamObserver<DeleteRedactionRuleResponse> responseObserver) {
      redactionRulesMap.remove(request.getRedactionRuleId());
      responseObserver.onNext(DeleteRedactionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void getAutomaticSecretRedactionStrategy(
        GetAutomaticSecretRedactionStrategyRequest request,
        StreamObserver<GetAutomaticSecretRedactionStrategyResponse> responseObserver) {
      responseObserver.onNext(
          GetAutomaticSecretRedactionStrategyResponse.newBuilder()
              .setEnabled(automaticSecretRedaction)
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void updateAutomaticSecretRedactionStrategy(
        UpdateAutomaticSecretRedactionStrategyRequest request,
        StreamObserver<UpdateAutomaticSecretRedactionStrategyResponse> responseObserver) {
      automaticSecretRedaction = request.getEnabled();
      responseObserver.onNext(UpdateAutomaticSecretRedactionStrategyResponse.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void getRedactionStrategyForType(
        GetRedactionStrategyForTypeRequest request,
        StreamObserver<GetRedactionStrategyForTypeResponse> responseObserver) {
      responseObserver.onNext(
          GetRedactionStrategyForTypeResponse.newBuilder()
              .build()
              .newBuilder()
              .setRedactionStrategy(headerRedactionStrategy)
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void updateRedactionStrategyForType(
        UpdateRedactionStrategyForTypeRequest request,
        StreamObserver<UpdateRedactionStrategyForTypeResponse> responseObserver) {
      headerRedactionStrategy = request.getRedactionStrategy();
      responseObserver.onNext(UpdateRedactionStrategyForTypeResponse.newBuilder().build());
      responseObserver.onCompleted();
    }
  }
}
