package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class DataSetConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");
  private final DataSetConfigRequestValidator dataSetConfigRequestValidator;

  public DataSetConfigRequestValidatorTest() {
    this.dataSetConfigRequestValidator = new DataSetConfigRequestValidator();
  }

  @Test
  void validateOrThrowNoDataSetInfo() {
    CreateDataSetRequest noDataSetInfoRequest = CreateDataSetRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataSetConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, noDataSetInfoRequest);
        });
  }

  @Test
  void validateOrThrowNoDataTypeIds() {
    DataSetInfo dataSetInfo = DataSetInfo.newBuilder().setName("name-1").setEnabled(true).build();
    CreateDataSetRequest request = CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build();
    dataSetConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowNoName() {
    DataSetInfo dataSetInfo =
        DataSetInfo.newBuilder().addAllDataTypeIds(List.of("1", "2")).setEnabled(true).build();
    CreateDataSetRequest request = CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataSetConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowNoColor() {
    DataSetInfo dataSetInfo =
        DataSetInfo.newBuilder()
            .setName("name-1")
            .addAllDataTypeIds(List.of("1", "2"))
            .setEnabled(true)
            .setColor("")
            .build();
    CreateDataSetRequest request = CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataSetConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowCompleteDataSetInfo() {
    DataSetInfo dataSetInfo =
        DataSetInfo.newBuilder()
            .addAllDataTypeIds(List.of("1", "2"))
            .setName("name-1")
            .setEnabled(true)
            .setColor("#F72202")
            .build();
    CreateDataSetRequest request = CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build();
    dataSetConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }
}
