package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ScopedGlobalConfigStatusChangeConverterTest {

  private final ScopedGlobalConfigStatusChangeConverter configStatusConverter =
      new ScopedGlobalConfigStatusChangeConverter();

  @Test
  public void testConvert() throws InvalidProtocolBufferException {

    getSampleScopes()
        .forEach(
            scope -> {
              ScopedAnomalyConfigStatusChange configStatus =
                  ScopedAnomalyConfigStatusChange.newBuilder()
                      .setConfigScope(scope)
                      .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
                      .build();
            });
  }

  private List<AnomalyConfigScope> getSampleScopes() {
    return List.of(
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build(),
        AnomalyConfigScope.newBuilder()
            .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
            .build(),
        AnomalyConfigScope.newBuilder()
            .setApiScope(
                AnomalyApiScope.newBuilder()
                    .setId("api1")
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                    .build())
            .build());
  }
}
