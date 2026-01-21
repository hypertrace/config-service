package ai.traceable.ipresolutionstrategy.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IpResolutionStrategyRequestValidatorTest {

  private IpResolutionStrategyRequestValidator validator;
  private RequestContext requestContext;
  private Context prevCtx;

  @BeforeEach
  void setup() {
    validator = new IpResolutionStrategyRequestValidator();
    requestContext = RequestContext.forTenantId("test-tenant");
    Context testCtx = Context.current().withValue(RequestContext.CURRENT, requestContext);
    prevCtx = testCtx.attach();
  }

  @AfterEach
  void tearDown() {
    Context.current().detach(prevCtx);
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest succeeds with valid data")
  void validateCreate_success() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setRawIp(RawIpExtraction.getDefaultInstance()))))
            .build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest throws when data is missing")
  void validateCreate_missingData() {
    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals(
        "Missing ip resolution strategy information inside create request",
        exception.getStatus().getDescription());
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest throws when strategy is missing")
  void validateCreate_missingStrategy() {
    IpResolutionStrategyConfigData data = IpResolutionStrategyConfigData.newBuilder().build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals(
        "No strategy provided in ip resolution input", exception.getStatus().getDescription());
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest throws when parsing strategy is missing")
  void validateCreate_missingParsingStrategy() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder().addSources(IpSource.newBuilder().build()))
            .build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals(
        "No parsing strategy provided for one of the inputs",
        exception.getStatus().getDescription());
  }

  @Test
  @DisplayName(
      "validateOrThrow for CreateRequest throws when parsing strategy has no extraction method")
  void validateCreate_emptyParsingStrategy() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(ParsingStrategy.newBuilder().build())))
            .build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals(
        "Parsing strategy has no information on where to extract the ip from",
        exception.getStatus().getDescription());
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest succeeds with keyword-based parsing strategy")
  void validateCreate_keywordBasedStrategy() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setKeywordBased(KeywordBasedExtraction.getDefaultInstance()))))
            .build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest succeeds with regex-based parsing strategy")
  void validateCreate_regexBasedStrategy() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setRegexBased(RegexBasedExtraction.getDefaultInstance()))))
            .build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for CreateRequest succeeds with multiple sources")
  void validateCreate_multipleSources() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setRawIp(RawIpExtraction.getDefaultInstance())))
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setKeywordBased(KeywordBasedExtraction.getDefaultInstance()))))
            .build();

    CreateIpResolutionStrategyConfigRequest request =
        CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for GetRequest succeeds with valid filter")
  void validateGet_success() {
    GetIpResolutionStrategyConfigsRequest request =
        GetIpResolutionStrategyConfigsRequest.newBuilder()
            .setFilter(IpResolutionStrategyFilter.getDefaultInstance())
            .build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for GetRequest throws when filter is missing")
  void validateGet_missingFilter() {
    GetIpResolutionStrategyConfigsRequest request =
        GetIpResolutionStrategyConfigsRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals(
        "Missing filter inside ip resolution strategy get request",
        exception.getStatus().getDescription());
  }

  @Test
  @DisplayName("validateOrThrow for UpdateRequest succeeds with valid id and data")
  void validateUpdate_success() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setRawIp(RawIpExtraction.getDefaultInstance()))))
            .build();

    UpdateIpResolutionStrategyConfigRequest request =
        UpdateIpResolutionStrategyConfigRequest.newBuilder().setId("test-id").setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for UpdateRequest throws when id is missing")
  void validateUpdate_missingId() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(
                IpResolutionStrategy.newBuilder()
                    .addSources(
                        IpSource.newBuilder()
                            .setParsingStrategy(
                                ParsingStrategy.newBuilder()
                                    .setRawIp(RawIpExtraction.getDefaultInstance()))))
            .build();

    UpdateIpResolutionStrategyConfigRequest request =
        UpdateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build();

    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for UpdateRequest throws when strategy is missing")
  void validateUpdate_missingStrategy() {
    IpResolutionStrategyConfigData data = IpResolutionStrategyConfigData.newBuilder().build();

    UpdateIpResolutionStrategyConfigRequest request =
        UpdateIpResolutionStrategyConfigRequest.newBuilder().setId("test-id").setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals(
        "No strategy provided in ip resolution input", exception.getStatus().getDescription());
  }

  @Test
  @DisplayName("validateOrThrow for DeleteRequest succeeds with valid id")
  void validateDelete_success() {
    DeleteIpResolutionStrategyConfigRequest request =
        DeleteIpResolutionStrategyConfigRequest.newBuilder().setId("test-id").build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  @DisplayName("validateOrThrow for DeleteRequest throws when id is missing")
  void validateDelete_missingId() {
    DeleteIpResolutionStrategyConfigRequest request =
        DeleteIpResolutionStrategyConfigRequest.newBuilder().build();

    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }
}
