package ai.traceable.region.config.service.regions;

import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.config.utils.refresh.FileVersionBasedRefresh;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;

@Slf4j
class DefaultRegionBuilder extends FileVersionBasedRefresh<Map<String, Region>>
    implements RegionBuilder {
  public static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  public static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  public static final String COUNTRY_CSV_HEADER = "country";
  public static final String ISO_CODE_CSV_HEADER = "country_iso_code";

  private final UuidGenerator uuidGenerator;

  @Inject
  DefaultRegionBuilder(
      LatestInstantNamedPathFinder latestInstantNamedPathFinder,
      UuidGenerator uuidGenerator,
      FileRefreshConfig fileRefreshConfig,
      boolean disabled) {
    super(latestInstantNamedPathFinder);
    this.uuidGenerator = uuidGenerator;
    if (disabled) {
      this.supplier = Collections::emptyMap;
    } else {
      initializeSupplier(fileRefreshConfig);
    }
  }

  @Override
  protected Map<String, Region> buildFromRecords(Iterable<CSVRecord> records) {
    Map<String, Region> regionIdToRegionMap = new HashMap<>();
    for (CSVRecord record : records) {
      try {
        long startIp = Long.parseLong(record.get(START_IP_INT_CSV_HEADER));
        long endIp = Long.parseLong(record.get(END_IP_INT_CSV_HEADER));
        String country = record.get(COUNTRY_CSV_HEADER).toLowerCase();
        String isoCode = record.get(ISO_CODE_CSV_HEADER).toLowerCase();

        String regionId = uuidGenerator.generateId(isoCode);

        if (!regionIdToRegionMap.containsKey(regionId)) {
          Region region =
              new Region(regionId, country, RegionType.COUNTRY, new ArrayList<>(), isoCode);
          regionIdToRegionMap.put(regionId, region);
        }

        Region region = regionIdToRegionMap.get(regionId);
        region.getIpV4Ranges().add(new IpV4Range(startIp, endIp));

      } catch (Exception e) {
        log.error("Unable to parse csv record {}", record, e);
      }
    }

    return ImmutableMap.<String, Region>builder().putAll(regionIdToRegionMap).build();
  }
}
