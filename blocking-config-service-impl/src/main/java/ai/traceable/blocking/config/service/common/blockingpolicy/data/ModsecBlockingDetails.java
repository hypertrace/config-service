package ai.traceable.blocking.config.service.common.blockingpolicy.data;

import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Builder
@Value
public class ModsecBlockingDetails implements BlockingDetails {
  @Singular List<String> ruleIds;

  @Override
  public <T> T accept(BlockingDetailsVisitor<T> v) {
    return v.visit(this);
  }
}
