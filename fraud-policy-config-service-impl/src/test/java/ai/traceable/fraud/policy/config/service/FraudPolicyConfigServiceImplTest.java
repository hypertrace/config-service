package ai.traceable.fraud.policy.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import ai.traceable.config.proto.utils.FieldMaskUtils;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.config.service.v1.FieldValue;
import ai.traceable.fraud.datamodel.config.service.v1.PrimitiveFieldValue;
import ai.traceable.fraud.policy.config.service.store.ApiAccessAnomalyConfigStore;
import ai.traceable.fraud.policy.config.service.store.ApiAccessAnomalyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStore;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.TemplateConfigStore;
import ai.traceable.fraud.policy.config.service.store.TemplateConfigStoreManager;
import ai.traceable.fraud.policy.config.service.v1.APISpec;
import ai.traceable.fraud.policy.config.service.v1.ApiAccessAnomalyConfig;
import ai.traceable.fraud.policy.config.service.v1.ApiCollection;
import ai.traceable.fraud.policy.config.service.v1.ApiReference;
import ai.traceable.fraud.policy.config.service.v1.CorrelationKey;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteTemplateRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteTemplateResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetTemplateListResponse;
import ai.traceable.fraud.policy.config.service.v1.GroupedConfig;
import ai.traceable.fraud.policy.config.service.v1.Template;
import ai.traceable.fraud.policy.config.service.v1.TimeObj;
import ai.traceable.fraud.policy.config.service.v1.TimeUnit;
import ai.traceable.fraud.policy.config.service.v1.TimeWindow;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateTemplateRequest;
import ai.traceable.fraud.policy.config.service.validation.ApiAccessAnomalyConfigServiceRequestValidator;
import ai.traceable.fraud.policy.config.service.validation.FraudPolicyConfigRequestValidator;
import ai.traceable.fraud.policy.config.service.validation.TemplateConfigRequestValidator;
import ai.traceable.fraud.query.model.v1.TypedParam;
import com.google.protobuf.FieldMask;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FraudPolicyConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";

  private FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub
      fraudPolicyConfigServiceBlockingStub;
  private FraudPolicyConfigStoreManager fraudPolicyConfigStoreManager;
  private TemplateConfigStoreManager templateConfigStoreManager;
  private ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager;

  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    if (testInfo.getTags().contains("fraudPolicy")) {
      this.mockGenericConfigService =
          new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();
    } else if (testInfo.getTags().contains("fraudTemplate")) {
      this.mockGenericConfigService =
          new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();
    } else if (testInfo.getTags().contains("apiAccessAnomaly")) {
      this.mockGenericConfigService =
          new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    }
  }

  @BeforeEach
  void beforeEach() {
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.fraudPolicyConfigStoreManager =
        new FraudPolicyConfigStoreManager(
            new FraudPolicyConfigStore(genericStub, eventGenerator), uuidGenerator);
    this.templateConfigStoreManager =
        new TemplateConfigStoreManager(
            new TemplateConfigStore(genericStub, eventGenerator), uuidGenerator);
    this.apiAccessAnomalyConfigStoreManager =
        new ApiAccessAnomalyConfigStoreManager(
            uuidGenerator, new ApiAccessAnomalyConfigStore(genericStub, eventGenerator));
    this.mockGenericConfigService
        .addService(
            new FraudPolicyConfigServiceImpl(
                fraudPolicyConfigStoreManager,
                new FraudPolicyConfigRequestValidator(),
                templateConfigStoreManager,
                new TemplateConfigRequestValidator(),
                apiAccessAnomalyConfigStoreManager,
                new ApiAccessAnomalyConfigServiceRequestValidator()))
        .start();

    this.fraudPolicyConfigServiceBlockingStub =
        FraudPolicyConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    when(uuidGenerator.generateRandomId()).thenReturn(UUID_1);
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  @Tag("fraudPolicy")
  void testFraudPolicyCRUD() {
    RequestContext requestContext = buildRequestContext();
    FraudPolicy createdFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .createFraudPolicy(
                        CreateFraudPolicyRequest.newBuilder()
                            .setFraudPolicy(FraudPolicy.newBuilder().setName("created"))
                            .build())
                    .getFraudPolicy());

    assertEquals(UUID_1, createdFraudPolicy.getId());

    FraudPolicy policy = FraudPolicy.newBuilder().setId(UUID_1).setName("updated").build();
    FieldMask output = FieldMaskUtils.generateFieldMask(policy);
    Assertions.assertEquals(2, output.getPathsCount());

    FraudPolicy updatedFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder().setId(UUID_1).setName("updated"))
                            .build())
                    .getFraudPolicy());

    assertEquals("updated", updatedFraudPolicy.getName());

    GetFraudPolicyListResponse fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.getDefaultInstance()));
    assertEquals(1, fraudPolicyListResponse.getFraudPolicyListCount());
    assertEquals(UUID_1, fraudPolicyListResponse.getFraudPolicyList(0).getId());

    FraudPolicy disabledFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setDisabled(true)
                                    .setName("disabled"))
                            .build())
                    .getFraudPolicy());

    assertEquals("disabled", disabledFraudPolicy.getName());
    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.getDefaultInstance()));
    assertEquals(0, fraudPolicyListResponse.getFraudPolicyListCount());
    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.newBuilder().setIncludeDisabled(true).build()));
    assertEquals(1, fraudPolicyListResponse.getFraudPolicyListCount());
    assertEquals(UUID_1, fraudPolicyListResponse.getFraudPolicyList(0).getId());

    GetFraudPolicyResponse fraudPolicyResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicy(
                    GetFraudPolicyRequest.newBuilder()
                        .setFraudPolicyId(UUID_1)
                        .setIncludeDisabled(true)
                        .build()));
    assertEquals(UUID_1, fraudPolicyResponse.getFraudPolicy().getId());

    DeleteFraudPolicyResponse deleteFraudPolicyResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.deleteFraudPolicy(
                    DeleteFraudPolicyRequest.newBuilder().addFraudPolicyIdList(UUID_1).build()));
    assertEquals(1, deleteFraudPolicyResponse.getFraudPolicyListCount());
    assertEquals(UUID_1, deleteFraudPolicyResponse.getFraudPolicyList(0).getId());

    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.newBuilder().setIncludeDisabled(true).build()));
    assertEquals(0, fraudPolicyListResponse.getFraudPolicyListCount());
  }

  @Test
  @Tag("fraudTemplate")
  public void testTemplateCRUD() {
    RequestContext requestContext = buildRequestContext();
    Template createdTemplate =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .createTemplate(
                        CreateTemplateRequest.newBuilder()
                            .setTemplate(Template.newBuilder().build())
                            .build())
                    .getTemplate());
    assertEquals(UUID_1, createdTemplate.getTemplateId());

    Template template =
        Template.newBuilder()
            .setTemplateId(UUID_1)
            .addDefaultTemplateVariables(
                TypedParam.newBuilder()
                    .setName("param-name")
                    .setValue(
                        FieldValue.newBuilder()
                            .setPrimitiveVal(
                                PrimitiveFieldValue.newBuilder()
                                    .setStringVal("param-value")
                                    .build())
                            .build())
                    .build())
            .build();
    FieldMask output = FieldMaskUtils.generateFieldMask(template);
    Assertions.assertEquals(6, output.getPathsCount());

    Template updatedTemplate =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateTemplate(
                        UpdateTemplateRequest.newBuilder()
                            .setTemplateId(UUID_1)
                            .setTemplate(template)
                            .build())
                    .getTemplate());

    assertEquals("param-name", updatedTemplate.getDefaultTemplateVariables(0).getName());
    assertEquals(
        "param-value",
        updatedTemplate.getDefaultTemplateVariables(0).getValue().getPrimitiveVal().getStringVal());

    GetTemplateListResponse templateListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getTemplateList(
                    GetTemplateListRequest.getDefaultInstance()));
    assertEquals(1, templateListResponse.getTemplateListCount());
    assertEquals(UUID_1, templateListResponse.getTemplateList(0).getTemplateId());

    DeleteTemplateResponse deleteTemplateResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.deleteTemplate(
                    DeleteTemplateRequest.newBuilder().addTemplateIdList(UUID_1).build()));
    assertEquals(1, deleteTemplateResponse.getTemplateListCount());
    assertEquals(UUID_1, deleteTemplateResponse.getTemplateList(0).getTemplateId());

    templateListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getTemplateList(
                    GetTemplateListRequest.newBuilder().build()));
    assertEquals(0, templateListResponse.getTemplateListCount());
  }

  @Test
  @Tag("apiAccessAnomaly")
  public void testCrud() {
    RequestContext requestContext = buildRequestContext();
    ApiAccessAnomalyConfig expected = new_config();

    ApiAccessAnomalyConfig created =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createApiAccessAnomalyConfig(
                        CreateApiAccessAnomalyConfigRequest.newBuilder()
                            .addGroupedConfigs(
                                GroupedConfig.newBuilder()
                                    .setApiReference(
                                        expected.getGroupedConfigs(0).getApiReference())
                                    .setPre(expected.getGroupedConfigs(0).getPre())
                                    .setPost(expected.getGroupedConfigs(0).getPost())
                                    .setLookbackTimeWindow(
                                        expected.getGroupedConfigs(0).getLookbackTimeWindow())
                                    .build())
                            .build())
                    .getConfig());

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    String id = created.getId();

    ApiAccessAnomalyConfig toUpdate = update_config(id);

    ApiAccessAnomalyConfig updated =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .updateApiAccessAnomalyConfig(
                        UpdateApiAccessAnomalyConfigRequest.newBuilder()
                            .setConfig(toUpdate)
                            .build())
                    .getConfig());

    assertEquals(id, updated.getId());
    assertEquals(
        toUpdate.getGroupedConfigs(0).getPre().getApis(0).getApiReference().getApiId(),
        updated.getGroupedConfigs(0).getPre().getApis(0).getApiReference().getApiId());

    ApiAccessAnomalyConfig config =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getApiAccessAnomalyConfig(
                        GetApiAccessAnomalyConfigRequest.newBuilder().setId(id).build())
                    .getConfig());

    assertEquals(id, config.getId());

    requestContext.call(
        () ->
            fraudPolicyConfigServiceBlockingStub.deleteApiAccessAnomalyConfig(
                DeleteApiAccessAnomalyConfigRequest.newBuilder().setId(id).build()));

    List<ApiAccessAnomalyConfig> configs =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getApiAccessAnomalyConfigs(
                        GetApiAccessAnomalyConfigsRequest.newBuilder().build())
                    .getConfigsList());

    assertEquals(0, configs.size());
  }

  private ApiAccessAnomalyConfig new_config() {
    ApiCollection pre =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_1").build())
                    .setWeight(1)
                    .build())
            .build();
    ApiCollection post =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_2").build())
                    .setWeight(1)
                    .build())
            .build();

    CorrelationKey correlationKey =
        CorrelationKey.newBuilder().addAttributes("ip_address").addAttributes("user_agent").build();

    TimeWindow lookbackWindow =
        TimeWindow.newBuilder()
            .setRelativeTime(
                TimeObj.newBuilder().setUnit(TimeUnit.TIME_UNIT_MINUTES).setValue(15).build())
            .build();
    ApiAccessAnomalyConfig config =
        ApiAccessAnomalyConfig.newBuilder()
            .addGroupedConfigs(
                GroupedConfig.newBuilder()
                    .setApiReference(
                        ApiReference.newBuilder().setApiId("target_api_id_100").build())
                    .setCorrelationKey(correlationKey)
                    .setLookbackTimeWindow(lookbackWindow)
                    .setPre(pre)
                    .setPost(post)
                    .build())
            .build();
    return config;
  }

  private ApiAccessAnomalyConfig update_config(String id) {
    ApiCollection pre =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_updated_1").build())
                    .setWeight(1)
                    .build())
            .build();
    ApiCollection post =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_2").build())
                    .setWeight(1)
                    .build())
            .build();

    CorrelationKey correlationKey =
        CorrelationKey.newBuilder().addAttributes("ip_address").addAttributes("user_agent").build();

    TimeWindow lookbackWindow =
        TimeWindow.newBuilder()
            .setRelativeTime(
                TimeObj.newBuilder().setUnit(TimeUnit.TIME_UNIT_MINUTES).setValue(15).build())
            .build();
    ApiAccessAnomalyConfig config =
        ApiAccessAnomalyConfig.newBuilder()
            .setId(id)
            .addGroupedConfigs(
                GroupedConfig.newBuilder()
                    .setApiReference(
                        ApiReference.newBuilder().setApiId("target_api_id_100").build())
                    .setCorrelationKey(correlationKey)
                    .setLookbackTimeWindow(lookbackWindow)
                    .setPre(pre)
                    .setPost(post)
                    .build())
            .build();
    return config;
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }
}
