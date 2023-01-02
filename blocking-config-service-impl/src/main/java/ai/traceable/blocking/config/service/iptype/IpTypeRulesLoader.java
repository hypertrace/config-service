package ai.traceable.blocking.config.service.iptype;

import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.blocking.config.service.v1.IpTypeRule.Builder;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.refresh.FileVersionBasedRefresh;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;

@Slf4j
public class IpTypeRulesLoader extends FileVersionBasedRefresh<List<IpTypeRule>> {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String IP_TYPE_CSV_HEADER = "IP Type";
  private static final Map<String, IpType> ipTypeMapping =
      Map.of(
          "IP_TYPE_VPN",
          IpType.IP_TYPE_VPN,
          "IP_TYPE_TOR",
          IpType.IP_TYPE_TOR,
          "IP_TYPE_BOT",
          IpType.IP_TYPE_BOT,
          "IP_TYPE_PROXY",
          IpType.IP_TYPE_PROXY,
          "IP_TYPE_HOSTING_PROVIDER",
          IpType.IP_TYPE_HOSTING_PROVIDER);

  @Inject
  public IpTypeRulesLoader(LatestInstantNamedPathFinder latestInstantNamedPathFinder) {
    super(latestInstantNamedPathFinder);
  }

  @Override
  public List<IpTypeRule> buildFromRecords(Iterable<CSVRecord> records) {
    if (!records.iterator().hasNext()) {
      log.warn("Received empty highrisk CSV, builder not returning ip types for blocking");
      return Collections.emptyList();
    }
    Map<IpType, IpTypeRule.Builder> ipTypeToRuleBuilderMap = new EnumMap<>(IpType.class);
    for (CSVRecord csvRecord : records) {
      try {
        String recordIpType = csvRecord.get(IP_TYPE_CSV_HEADER);
        IpType ipType = ipTypeMapping.get(recordIpType);
        if (ipType == null) {
          log.error("Recieved unknown ip type {}, skipping", recordIpType);
          continue;
        }
        if (!ipTypeToRuleBuilderMap.containsKey(ipType)) {
          ipTypeToRuleBuilderMap.put(ipType, IpTypeRule.newBuilder().setIpType(ipType));
        }

        long startIp = Long.parseLong(csvRecord.get(START_IP_INT_CSV_HEADER));
        long endIp = Long.parseLong(csvRecord.get(END_IP_INT_CSV_HEADER));
        IpRange ipRange =
            (startIp == endIp)
                ? IpRange.newBuilder().setIpv4Address((int) startIp).build()
                : IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(startIp).setEndIp(endIp))
                    .build();

        ipTypeToRuleBuilderMap.get(ipType).addIpRanges(ipRange);

      } catch (Exception e) {
        log.error("Unable to parse csv record {}", csvRecord, e);
      }
    }
    return ipTypeToRuleBuilderMap.values().stream()
        .map(Builder::build)
        .collect(Collectors.toUnmodifiableList());
  }
}
