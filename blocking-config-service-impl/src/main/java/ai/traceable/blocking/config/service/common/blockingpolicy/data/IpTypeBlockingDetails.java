package ai.traceable.blocking.config.service.common.blockingpolicy.data;

import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Builder
@Value
public class IpTypeBlockingDetails implements BlockingDetails {
  @Singular List<IpLocationType> ipTypes;

  @Override
  public <T> T accept(BlockingDetailsVisitor<T> v) {
    return v.visit(this);
  }
}
