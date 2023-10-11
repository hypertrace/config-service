package ai.traceable.blocking.config.service.common.blockingpolicy.data;

import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Builder
@Value
public class CombinationBlockingDetails implements BlockingDetails {
  @Singular List<BlockingDetails> blockingDetailsOperands;
  Operator operator;

  public enum Operator {
    AND,
    OR,
    NOT
  }

  @Override
  public <T> T accept(BlockingDetailsVisitor<T> v) {
    return v.visit(this);
  }
}
