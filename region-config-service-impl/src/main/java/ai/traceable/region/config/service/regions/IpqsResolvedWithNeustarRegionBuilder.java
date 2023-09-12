package ai.traceable.region.config.service.regions;

import static ai.traceable.region.config.service.regions.DefaultRegionBuilder.COUNTRY_CSV_HEADER;
import static ai.traceable.region.config.service.regions.DefaultRegionBuilder.END_IP_INT_CSV_HEADER;
import static ai.traceable.region.config.service.regions.DefaultRegionBuilder.ISO_CODE_CSV_HEADER;
import static ai.traceable.region.config.service.regions.DefaultRegionBuilder.START_IP_INT_CSV_HEADER;

import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.config.utils.refresh.FileVersionBasedRefresh;
import ai.traceable.region.config.service.RegionConfigServiceConfig;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;

/** takes ipqs as primary data source and searches for mismatch in country from neustar to drop */
@Slf4j
@Singleton
public class IpqsResolvedWithNeustarRegionBuilder implements RegionBuilder {
  private final LatestInstantNamedPathFinder latestInstantNamedPathFinder;
  private final Supplier<Map<String, Region>> resolvedSupplier;
  private final UuidGenerator uuidGenerator;

  @Inject
  IpqsResolvedWithNeustarRegionBuilder(
      LatestInstantNamedPathFinder latestInstantNamedPathFinder,
      UuidGenerator uuidGenerator,
      RegionConfigServiceConfig config) {
    this.latestInstantNamedPathFinder = latestInstantNamedPathFinder;
    this.uuidGenerator = uuidGenerator;
    if (!config.getIpqsNeustarResolutionEnabled()) {
      resolvedSupplier = Collections::emptyMap;
      return;
    }
    FileRefreshConfig ipqsCountriesDataConfig = config.getIpqsCountriesDataConfig();
    FileRefreshConfig neustarCountriesDataConfig = config.getNeustarCountriesDataConfig();
    // if no refresh configured for both, then don't initialise cache
    if (Objects.isNull(ipqsCountriesDataConfig.getVersionRefreshDuration())
        && Objects.isNull(neustarCountriesDataConfig.getVersionRefreshDuration())) {
      resolvedSupplier =
          Suppliers.memoize(
              () -> getResolvedRegions(ipqsCountriesDataConfig, neustarCountriesDataConfig));
      return;
    }
    // cache the merged results with same frequency as ipqs, if ipqs doesn't have refresh then pick
    // neustar
    LoadingCache<FileRefreshConfig, Map<String, Region>> cache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(
                Objects.nonNull(ipqsCountriesDataConfig.getVersionRefreshDuration())
                    ? ipqsCountriesDataConfig.getVersionRefreshDuration().toMinutes()
                    : neustarCountriesDataConfig.getVersionRefreshDuration().toMinutes(),
                TimeUnit.MINUTES)
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(
                        () ->
                            getResolvedRegions(
                                ipqsCountriesDataConfig, neustarCountriesDataConfig)),
                    Executors.newSingleThreadExecutor()));
    // seed cache
    cache.put(
        ipqsCountriesDataConfig,
        getResolvedRegions(ipqsCountriesDataConfig, neustarCountriesDataConfig));
    resolvedSupplier = () -> cache.getUnchecked(ipqsCountriesDataConfig);
  }

  @Override
  public Supplier<Map<String, Region>> getLatestDataSupplier() {
    return resolvedSupplier;
  }

  private Map<String, Region> getResolvedRegions(
      FileRefreshConfig ipqsConfig, FileRefreshConfig neustarConfig) {
    TreeMap<Long, EndIpRegionEntry> neustarRangeMap =
        FileVersionBasedRefresh.fetchRecordsAndApplyFunction(
            neustarConfig, latestInstantNamedPathFinder, this::getNeustarRangeMap);
    return FileVersionBasedRefresh.fetchRecordsAndApplyFunction(
        ipqsConfig,
        latestInstantNamedPathFinder,
        (records) -> getResolvedIpRegions(records, neustarRangeMap));
  }

  private TreeMap<Long, EndIpRegionEntry> getNeustarRangeMap(Iterable<CSVRecord> records) {
    TreeMap<Long, EndIpRegionEntry> rangeMap = new TreeMap<>();
    for (CSVRecord record : records) {
      try {
        long startIp = Long.parseLong(record.get(START_IP_INT_CSV_HEADER));
        long endIp = Long.parseLong(record.get(END_IP_INT_CSV_HEADER));
        String isoCode = record.get(ISO_CODE_CSV_HEADER).toLowerCase();
        rangeMap.put(startIp, new EndIpRegionEntry(endIp, isoCode));
      } catch (Exception e) {
        log.error("Unable to parse csv record {}", record, e);
      }
    }
    return rangeMap;
  }

  private Map<String, Region> getResolvedIpRegions(
      Iterable<CSVRecord> records, TreeMap<Long, EndIpRegionEntry> neustarRangeMap) {
    Map<String, String> countryCodeToName = new HashMap<>();
    Map<String, List<IpV4Range>> resolvedRegionIpRanges = new HashMap<>(); // isocode is key
    LongCounter totalIpqsIps = new LongCounter();
    LongCounter totalResolvedIps = new LongCounter();
    LongCounter totalMismatchedIps = new LongCounter();
    for (CSVRecord record : records) {
      try {
        long startIp = Long.parseLong(record.get(START_IP_INT_CSV_HEADER));
        long endIp = Long.parseLong(record.get(END_IP_INT_CSV_HEADER));
        String country = record.get(COUNTRY_CSV_HEADER).toLowerCase();
        String isoCode = record.get(ISO_CODE_CSV_HEADER).toLowerCase();
        countryCodeToName.put(isoCode, country);
        resolvedRegionIpRanges.computeIfAbsent(isoCode, c -> new ArrayList<>());
        IpV4Range ipqsIpV4Range = new IpV4Range(startIp, endIp);
        totalIpqsIps.increment(ipqsIpV4Range.getIpsCount());
        resolveIpqsIpRange(
            neustarRangeMap,
            resolvedRegionIpRanges,
            isoCode,
            ipqsIpV4Range,
            totalResolvedIps,
            totalMismatchedIps);
      } catch (Exception e) {
        log.error("Unable to parse csv record {}", record, e);
      }
    }
    log.info(
        "Total Ipqs IPs {}, total Resolved Ips {}, total mismatched Ips {}, drop rate % {}",
        totalIpqsIps.getCount(),
        totalResolvedIps.getCount(),
        totalMismatchedIps.getCount(),
        100 * (totalMismatchedIps.getCount()) / ((double) (totalIpqsIps.getCount())));
    return resolvedRegionIpRanges.entrySet().stream()
        .map(
            e ->
                Map.entry(
                    e.getKey(),
                    new Region(
                        uuidGenerator.generateId(e.getKey()),
                        countryCodeToName.get(e.getKey()),
                        RegionType.COUNTRY,
                        e.getValue(),
                        e.getKey())))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private void resolveIpqsIpRange(
      TreeMap<Long, EndIpRegionEntry> neustarRangeMap,
      Map<String, List<IpV4Range>> resolvedRegionIpRanges,
      String ipqsRegion,
      IpV4Range ipqsIpV4Range,
      LongCounter totalResolvedIps,
      LongCounter totalMismatchedIps) {
    while (true) {
      // ipqs = 5-15
      Map.Entry<Long, EndIpRegionEntry> floorEntry =
          neustarRangeMap.floorEntry(ipqsIpV4Range.getStartIp());
      if (floorEntry != null) {
        // ipqs = 5-15, neustar = 3-x, 5-x
        long neustarEndIp = floorEntry.getValue().getEndIp();
        String neustarRegion = floorEntry.getValue().getCountryIsoCode();

        if (neustarEndIp >= ipqsIpV4Range.getEndIp()) {
          // ipqs = 5-15, neustar = 3-15, 3-17, 5-15, 5-17
          addToMap(
              resolvedRegionIpRanges,
              ipqsRegion,
              neustarRegion,
              ipqsIpV4Range,
              totalResolvedIps,
              totalMismatchedIps);
          return;
        } else if (neustarEndIp
            >= ipqsIpV4Range.getStartIp()) { // neustartEndIp < ipqsIpV4Range.getEndIp()
          // ipqs = 5-15, neustar = 3-5, 3-8, 5-8
          IpV4Range resolvedIpRange = new IpV4Range(ipqsIpV4Range.getStartIp(), neustarEndIp);
          addToMap(
              resolvedRegionIpRanges,
              ipqsRegion,
              neustarRegion,
              resolvedIpRange,
              totalResolvedIps,
              totalMismatchedIps);

          ipqsIpV4Range = new IpV4Range(neustarEndIp + 1, ipqsIpV4Range.getEndIp());
          continue;
        }
        // else -> neustartEndIp < ipqsIpV4Range.getEndIp() && neustarEndIp <
        // ipqsIpV4Range.getStartIp()
        // ipqs = 5-15, neustar = 3-4 - no intersection, check ceilEntry

      }

      // ipqs = 5-15
      Map.Entry<Long, EndIpRegionEntry> ceilEntry =
          neustarRangeMap.ceilingEntry(ipqsIpV4Range.getStartIp());
      if (ceilEntry != null) {
        // ipqs = 5-15, neustar = 7-x, 15-x, 17-x (because 5-x already handled within floorEntry and
        // returned)
        long neustarStartIp = ceilEntry.getKey();
        long neustarEndIp = ceilEntry.getValue().getEndIp();
        String neustarRegion = ceilEntry.getValue().getCountryIsoCode();

        if (neustarStartIp > ipqsIpV4Range.getEndIp()) {
          // ipqs = 5-15, neustar = 17-x - no intersection with floor or ceil, so not present in
          // neustar
          addToResolvedMap(resolvedRegionIpRanges, ipqsRegion, ipqsIpV4Range, totalResolvedIps);
          return;
        } else if (neustarEndIp <= ipqsIpV4Range.getEndIp()) {
          // ipqs = 5-15, neustar = 7-13, 7-15, 15-15
          // 5-6 not present in neustar, 7-13 intersecting with neustar
          IpV4Range missingIpRange = new IpV4Range(ipqsIpV4Range.getStartIp(), neustarStartIp - 1);
          addToResolvedMap(resolvedRegionIpRanges, ipqsRegion, missingIpRange, totalResolvedIps);

          IpV4Range resolvedIpRange = new IpV4Range(neustarStartIp, neustarEndIp);
          addToMap(
              resolvedRegionIpRanges,
              ipqsRegion,
              neustarRegion,
              resolvedIpRange,
              totalResolvedIps,
              totalMismatchedIps);
          if (neustarEndIp < ipqsIpV4Range.getEndIp()) {
            ipqsIpV4Range = new IpV4Range(neustarEndIp + 1, ipqsIpV4Range.getEndIp());
            continue;
          }
          return;
        } else { // neustarEndIp > ipqsIpV4Range.getEndIp()
          // ipqs = 5-15, neustar = 7-17, 15-17
          // 5-6 not present in neustar, 7-15 intersecting with neustar
          IpV4Range missingIpRange = new IpV4Range(ipqsIpV4Range.getStartIp(), neustarStartIp - 1);
          addToResolvedMap(resolvedRegionIpRanges, ipqsRegion, missingIpRange, totalResolvedIps);

          IpV4Range resolvedIpRange = new IpV4Range(neustarStartIp, ipqsIpV4Range.getEndIp());
          addToMap(
              resolvedRegionIpRanges,
              ipqsRegion,
              neustarRegion,
              resolvedIpRange,
              totalResolvedIps,
              totalMismatchedIps);
          return;
        }
      } else {
        // range not found in neustar ceil or floor entries
        addToResolvedMap(resolvedRegionIpRanges, ipqsRegion, ipqsIpV4Range, totalResolvedIps);
        return;
      }
    }
  }

  private void addToMap(
      Map<String, List<IpV4Range>> resolvedRegionIpRanges,
      String ipqsRegion,
      String neustarRegion,
      IpV4Range ipV4Range,
      LongCounter totalResolvedIps,
      LongCounter totalMismatchedIps) {
    if (ipqsRegion.equals(neustarRegion)) {
      addToRangeWithMerging(resolvedRegionIpRanges.get(ipqsRegion), ipV4Range);
      totalResolvedIps.increment(ipV4Range.getIpsCount());
    } else {
      totalMismatchedIps.increment(ipV4Range.getIpsCount());
    }
  }

  private void addToResolvedMap(
      Map<String, List<IpV4Range>> resolvedRegionIpRanges,
      String ipqsRegion,
      IpV4Range ipV4Range,
      LongCounter totalResolvedIps) {
    addToRangeWithMerging(resolvedRegionIpRanges.get(ipqsRegion), ipV4Range);
    totalResolvedIps.increment(ipV4Range.getIpsCount());
  }

  private void addToRangeWithMerging(List<IpV4Range> ranges, IpV4Range ipV4Range) {
    int size = ranges.size();
    if (size > 0 && ranges.get(size - 1).getEndIp() == ipV4Range.getStartIp() - 1) {
      // can merge
      IpV4Range removedRange = ranges.remove(size - 1);
      ranges.add(new IpV4Range(removedRange.getStartIp(), ipV4Range.getEndIp()));
    } else {
      ranges.add(ipV4Range);
    }
  }

  @Value
  private static class EndIpRegionEntry {
    long endIp;
    String countryIsoCode;
  }

  @Getter
  private static class LongCounter {
    long count = 0;

    void increment(long amount) {
      count += amount;
    }
  }
}
