package ai.traceable.anomaly.config.service.global.status;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class GlobalConfigStatusConverterTest {

  private final GlobalConfigStatusConverter configStatusConverter =
      new GlobalConfigStatusConverter();

  @Test
  public void testConvert() throws InvalidProtocolBufferException {
    AnomalyConfigStatus defaultStatus =
        AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build();

    AnomalyConfigStatusChange configStatusChange;
    Value value;

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    value = configStatusConverter.convert(configStatusChange);
    assertEquals(configStatusChange, configStatusConverter.convert(value));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(false).build(),
        configStatusConverter.convert(value, AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build(),
        configStatusConverter.convert(value, defaultStatus));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    value = configStatusConverter.convert(configStatusChange);
    assertEquals(configStatusChange, configStatusConverter.convert(value));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(false).build(),
        configStatusConverter.convert(value, AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(false).build(),
        configStatusConverter.convert(value, defaultStatus));

    configStatusChange =
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(false).build();
    value = configStatusConverter.convert(configStatusChange);
    assertEquals(configStatusChange, configStatusConverter.convert(value));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(false).build(),
        configStatusConverter.convert(value, AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(false).build(),
        configStatusConverter.convert(value, defaultStatus));
  }
}
