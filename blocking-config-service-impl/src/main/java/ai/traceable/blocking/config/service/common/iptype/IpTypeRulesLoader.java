package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpRangeInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpType;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpTypeRuleInfoBuilder;
import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.config.utils.refresh.FileVersionBasedRefresh;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.micrometer.core.instrument.Counter;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
@Slf4j
public class IpTypeRulesLoader extends FileVersionBasedRefresh<Map<IpType, IpTypeRuleInfo>> {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String IP_TYPE_CSV_HEADER = "IP Type";
  private static final Map<String, IpType> ipTypeMapping =
      Map.of(
          "IP_TYPE_VPN",
          IpType.ANONYMOUS_VPN,
          "IP_TYPE_TOR",
          IpType.TOR_EXIT_NODE,
          "IP_TYPE_BOT",
          IpType.BOT,
          "IP_TYPE_PROXY",
          IpType.PUBLIC_PROXY,
          "IP_TYPE_HOSTING_PROVIDER",
          IpType.HOSTING_PROVIDER);
  private static final String IPQS_NEUSTAR_LOAD_FAILURE = "ipqs.neustar.load.failure";
  private final Counter counter;

  @Inject
  public IpTypeRulesLoader(
      LatestInstantNamedPathFinder latestInstantNamedPathFinder,
      FileRefreshConfig fileRefreshConfig) {
    super(latestInstantNamedPathFinder);
    this.counter = PlatformMetricsRegistry.registerCounter(IPQS_NEUSTAR_LOAD_FAILURE, null);
    try {
      initializeSupplier(fileRefreshConfig);
    } catch (Exception e) {
      counter.increment();
      this.supplier = Collections::emptyMap;
    }
  }

  @Override
  protected Map<IpType, IpTypeRuleInfo> buildFromRecords(Iterable<CSVRecord> records) {
    if (!records.iterator().hasNext()) {
      log.warn("Received empty highrisk CSV, builder not returning ip types for blocking");
      return Collections.emptyMap();
    }
    Map<IpType, IpTypeRuleInfoBuilder> ipTypeRulesMap = new EnumMap<>(IpType.class);
    for (CSVRecord csvRecord : records) {
      try {
        String recordIpType = csvRecord.get(IP_TYPE_CSV_HEADER);
        IpType ipType = ipTypeMapping.get(recordIpType);
        if (ipType == null) {
          log.error("Received unknown ip type {}, skipping", recordIpType);
          continue;
        }
        if (!ipTypeRulesMap.containsKey(ipType)) {
          ipTypeRulesMap.put(ipType, IpTypeRuleInfo.builder().ipType(ipType));
        }

        int startIp = Integer.parseUnsignedInt(csvRecord.get(START_IP_INT_CSV_HEADER));
        int endIp = Integer.parseUnsignedInt(csvRecord.get(END_IP_INT_CSV_HEADER));
        if (startIp == endIp) {
          ipTypeRulesMap.get(ipType).ipv4Address(startIp);
        } else {
          ipTypeRulesMap.get(ipType).ipv4Range(new IpRangeInfo(startIp, endIp));
        }
      } catch (Exception e) {
        log.error("Unable to parse csv record {}", csvRecord, e);
      }
    }

    return ipTypeRulesMap.values().stream()
        .map(IpTypeRuleInfoBuilder::build)
        .collect(Collectors.toUnmodifiableMap(IpTypeRuleInfo::getIpType, Function.identity()));
  }
}
