package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpRangeInfo;
import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.config.utils.refresh.FileVersionBasedRefresh;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;

@Singleton
@Slf4j
public class IpTypeRulesLoader extends FileVersionBasedRefresh<List<IpTypeRuleInfo>> {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String IP_TYPE_CSV_HEADER = "IP Type";
  private static final Map<String, IpLocationType> ipTypeMapping =
      Map.of(
          "IP_TYPE_VPN",
          IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN,
          "IP_TYPE_TOR",
          IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE,
          "IP_TYPE_BOT",
          IpLocationType.IP_LOCATION_TYPE_BOT,
          "IP_TYPE_PROXY",
          IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY,
          "IP_TYPE_HOSTING_PROVIDER",
          IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER);

  @Inject
  public IpTypeRulesLoader(
      LatestInstantNamedPathFinder latestInstantNamedPathFinder,
      FileRefreshConfig fileRefreshConfig) {
    super(latestInstantNamedPathFinder);
    initializeSupplier(fileRefreshConfig);
  }

  @Override
  protected List<IpTypeRuleInfo> buildFromRecords(Iterable<CSVRecord> records) {
    if (!records.iterator().hasNext()) {
      log.warn("Received empty highrisk CSV, builder not returning ip types for blocking");
      return Collections.emptyList();
    }
    Map<IpLocationType, IpTypeRuleInfo> ipTypeToRuleMap = new EnumMap<>(IpLocationType.class);
    for (CSVRecord csvRecord : records) {
      try {
        String recordIpType = csvRecord.get(IP_TYPE_CSV_HEADER);
        IpLocationType ipType = ipTypeMapping.get(recordIpType);
        if (ipType == null) {
          log.error("Received unknown ip type {}, skipping", recordIpType);
          continue;
        }
        if (!ipTypeToRuleMap.containsKey(ipType)) {
          ipTypeToRuleMap.put(ipType, new IpTypeRuleInfo(ipType));
        }

        int startIp = Integer.parseUnsignedInt(csvRecord.get(START_IP_INT_CSV_HEADER));
        int endIp = Integer.parseUnsignedInt(csvRecord.get(END_IP_INT_CSV_HEADER));
        if (startIp == endIp) {
          ipTypeToRuleMap.get(ipType).getIpv4Addresses().add(startIp);
        } else {
          ipTypeToRuleMap.get(ipType).getIpv4Ranges().add(new IpRangeInfo(startIp, endIp));
        }

      } catch (Exception e) {
        log.error("Unable to parse csv record {}", csvRecord, e);
      }
    }
    return ipTypeToRuleMap.values().stream().collect(Collectors.toUnmodifiableList());
  }
}
