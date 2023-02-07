package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class IpTypeRuleInfo {
  public IpTypeRuleInfo(IpLocationType ipLocationType) {
    this.ipLocationType = ipLocationType;
    this.ipv4Addresses = new ArrayList<>();
    this.ipv4Ranges = new ArrayList<>();
  }

  IpLocationType ipLocationType;
  List<IpRangeInfo> ipv4Ranges;
  List<Integer> ipv4Addresses;

  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  public static class IpRangeInfo {
    int start;
    int end;
  }
}
