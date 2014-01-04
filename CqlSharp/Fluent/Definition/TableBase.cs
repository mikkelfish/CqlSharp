using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Definition
{
    public abstract class TableBase<T> where T: TableBase<T>
    {
        protected readonly List<IOption> options = new List<IOption>();
        protected bool clusteringOrder;
        protected bool compactStorage;

        protected string getOptionString()
        {
            var toRet = new StringBuilder();
            var hasOption = false;
            toRet.Append(" WITH ");
            if (this.clusteringOrder)
            {
                toRet.Append("CLUSTERING ORDER");
                hasOption = true;
            }

            if (this.compactStorage)
            {
                if (hasOption)
                    toRet.Append(" AND ");
                toRet.Append("COMPACT STORAGE");
                hasOption = true;
            }

            if (this.options.Count == 0)
            {
                return toRet.ToString(); //We have no more options so we're done
            }

            if (hasOption)
                toRet.Append(" AND ");

            for (int i = 0; i < this.options.Count - 1; i++)
            {
                toRet.AppendFormat("{0} AND ", this.options[i].BuildString);
            }

            toRet.AppendFormat("{0}", this.options.Last().BuildString);
            return toRet.ToString();
        }

        /// <summary>
        /// It allows to define the ordering of rows on disk. It takes the list of the clustering key names with, for each of them, the on-disk order (Ascending or descending). Note that this option affects what ORDER BY are allowed during SELECT.
        /// </summary>
        /// <param name="clusterOrdering">Default: false</param>
        /// <returns></returns>
        public T WithClusterOrdering(bool clusterOrdering)
        {
            this.clusteringOrder = clusterOrdering;
            return this as T;
        }

        /// <summary>
        /// A human readable comment describing the column family.
        /// </summary>
        /// <param name="comment">Default: ""</param>
        /// <returns></returns>
        public T WithComment(string comment)
        {
            this.options.Add(new SimpleOption<string>("comment", comment));
            return this as T;
        }

        /// <summary>
        /// Specifies the probability with which read repairs should be invoked on non-quorum reads. The value must be between 0 and 1
        /// </summary>
        /// <param name="repairChance">Default: 0.1</param>
        /// <returns></returns>
        public T WithReadRepairChance(double repairChance)
        {
            this.options.Add(new SimpleOption<double>("read_repair_chance", repairChance, rc => rc >= 0 && rc <= 1));
            return this as T;
        }

        /// <summary>
        /// Specifies the probability with which read repairs should be invoked over all replicas in the current data center
        /// </summary>
        /// <param name="dcRepairChance">Default: 0</param>
        /// <returns></returns>
        public T WithDcLocalRepairChance(double dcRepairChance)
        {
            this.options.Add(new SimpleOption<double>("dclocal_read_repair_chance", dcRepairChance, drc => drc >= 0 && drc <= 1));
            return this as T;
        }

        /// <summary>
        /// Specifies the time to wait before garbage collecting tombstones (deletion markers). Defaults to 864000, or 10 days, which allows a great deal of time for consistency to be achieved prior to deletion. In many deployments this interval can be reduced, and in a single-node cluster it can be safely set to zero.
        /// </summary>
        /// <param name="seconds">Default: 864000</param>
        /// <returns></returns>
        public T WithGcGraceSeconds(long seconds)
        {
            this.options.Add(new SimpleOption<long>("gc_grace_seconds", seconds, sc => sc >= 0));
            return this as T;
        }

        /// <summary>
        /// The target probability of false positive of the sstable bloom filters. Said bloom filters will be sized to provide the provided probability (thus lowering this value impact the size of bloom filters in-memory and on-disk)
        /// </summary>
        /// <param name="chance">Default: 0.00075</param>
        /// <returns></returns>
        public T WithBloomFilterFalsePositiveChance(double chance)
        {
            this.options.Add(new SimpleOption<double>("bloom_filter_fp_chance", chance, c => c >= 0 && c <= 1));
            return this as T;
        }

        /// <summary>
        /// Whether to replicate data on write. This can only be set to false for tables with counters values. Disabling this is dangerous and can result in random lose of counters, don’t disable unless you are sure to know what you are doing
        /// </summary>
        /// <param name="replicate">Default: true</param>
        /// <returns></returns>
        public T WithReplicateOnWrite(bool replicate)
        {
            this.options.Add(new SimpleOption<bool>("replicate_on_write", replicate));
            return this as T;
        }

        /// <summary>
        /// Whether to cache keys (“key cache”) and/or rows (“row cache”) for this table. Valid values are: all, keys_only, rows_only and none.
        /// </summary>
        /// <param name="caching">Default: keys_only</param>
        /// <returns></returns>
        public T WithCaching(string caching)
        {
            this.options.Add(new SimpleOption<string>("caching", caching, c => c == "all" || c == "keys_only" || c == "rows_only" || c == "none"));
            return this as T;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tombstoneThreshold">A ratio such that if a sstable has more than this ratio of gcable tombstones over all contained columns, the sstable will be compacted (with no other sstables) for the purpose of purging those tombstones. Default 0.2</param>
        /// <param name="tombstoneCompactionInterval">The minimum time to wait after an sstable creation time before considering it for “tombstone compaction”, where “tombstone compaction” is the compaction triggered if the sstable has more gcable tombstones than tombstone_threshold. Default 86400 seconds (1 day)</param>
        /// <param name="ssTableSize">The target size (in MB) for sstables in the leveled strategy. Note that while sstable sizes should stay less or equal to sstable_size_in_mb, it is possible to exceptionally have a larger sstable as during compaction, data for a given partition key are never split into 2 sstables. Default 5 megs</param>
        /// <returns></returns>
        public T WithLeveledCompaction(double tombstoneThreshold = 0.2, long tombstoneCompactionInterval = 86400, int ssTableSize = 5)
        {
            var toInclude = new List<IOption> { new SimpleOption<string>("class", "LeveledCompactionStrategy") };
            if (tombstoneThreshold != 0.2)
                toInclude.Add(new SimpleOption<double>("tombstone_threshold", tombstoneThreshold, tt => tt >= 0 && tt <= 1));
            if (tombstoneCompactionInterval != 86400)
                toInclude.Add(new SimpleOption<double>("tombstone_compaction_interval", tombstoneCompactionInterval, tt => tt > 0));
            if (ssTableSize != 5)
                toInclude.Add(new SimpleOption<int>("sstable_size_in_mb", ssTableSize, ss => ss > 0));
            this.options.Add(new MapOption("compaction", toInclude));
            return this as T;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tombstoneThreshold">A ratio such that if a sstable has more than this ratio of gcable tombstones over all contained columns, the sstable will be compacted (with no other sstables) for the purpose of purging those tombstones. Default 0.2</param>
        /// <param name="tombstoneCompactionInterval">The minimum time to wait after an sstable creation time before considering it for “tombstone compaction”, where “tombstone compaction” is the compaction triggered if the sstable has more gcable tombstones than tombstone_threshold. Default 86400 seconds (1 day)</param>
        /// <param name="minSSTableSize">The size tiered strategy groups SSTables to compact in buckets. A bucket groups SSTables that differs from less than 50% in size. However, for small sizes, this would result in a bucketing that is too fine grained. min_sstable_size defines a size threshold (in bytes) below which all SSTables belong to one unique bucket</param>
        /// <param name="minThreshold">Minimum number of SSTables needed to start a minor compaction.</param>
        /// <param name="maxThreshold">Maximum number of SSTables processed by one minor compaction</param>
        /// <param name="bucketLow">Size tiered consider sstables to be within the same bucket if their size is within [average_size * bucket_low, average_size * bucket_high ] (i.e the default groups sstable whose sizes diverges by at most 50%)</param>
        /// <param name="bucketHigh">Size tiered consider sstables to be within the same bucket if their size is within [average_size * bucket_low, average_size * bucket_high ] (i.e the default groups sstable whose sizes diverges by at most 50%)</param>
        /// <returns></returns>
        public T WithSizeTieredCompaction(double tombstoneThreshold = 0.2, long tombstoneCompactionInterval = 86400, int minSSTableSize = 52428800,
            int minThreshold = 4, int maxThreshold = 32, double bucketLow = 0.5, double bucketHigh = 1.5)
        {
            var toInclude = new List<IOption> { new SimpleOption<string>("class", "SizeTieredCompactionStrategy") };
            if (tombstoneThreshold != 0.2)
                toInclude.Add(new SimpleOption<double>("tombstone_threshold", tombstoneThreshold, tt => tt >= 0 && tt <= 1));
            if (tombstoneCompactionInterval != 86400)
                toInclude.Add(new SimpleOption<long>("tombstone_compaction_interval", tombstoneCompactionInterval, tt => tt >= 0));
            if (minSSTableSize != 52428800)
                toInclude.Add(new SimpleOption<long>("min_sstable_size", minSSTableSize, tt => tt > 0));
            if (minThreshold != 4)
                toInclude.Add(new SimpleOption<int>("min_threshold", minThreshold, tt => tt > 0));
            if (maxThreshold != 32)
                toInclude.Add(new SimpleOption<int>("max_threshold", maxThreshold, tt => tt > 0));
            if (bucketLow != 0.5)
                toInclude.Add(new SimpleOption<double>("bucket_low", bucketLow, tt => tt > 0));
            if (bucketLow != 0.5)
                toInclude.Add(new SimpleOption<double>("bucket_high", bucketHigh, tt => tt > 0));

            this.options.Add(new MapOption("compaction", toInclude));
            return this as T;
        }

        /// <summary>
        /// For use with a custom compaction strategy
        /// </summary>
        /// <param name="name">Name of the strategy</param>
        /// <param name="parameters">Parameters</param>
        /// <returns></returns>
        public T WithCustomCompaction(string name, Dictionary<string, object> parameters)
        {
            var toInclude = new List<IOption> { new SimpleOption<string>("class", name) };
            foreach (var parameter in parameters)
            {
                toInclude.Add(new SimpleOption<object>(parameter.Key, parameter.Value));
            }
            this.options.Add(new MapOption("compaction", toInclude));
            return this as T;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="algorithm">The compression algorithm to use. Default compressor are: SnappyCompressor and DeflateCompressor. Use an empty string ('') to disable compression. Custom compressor can be provided by specifying the full class name as a string constant.</param>
        /// <param name="chunkLength">On disk SSTables are compressed by block (to allow random reads). This defines the size (in KB) of said block. Bigger values may improve the compression rate, but increases the minimum size of data to be read from disk for a read</param>
        /// <param name="crcCheckChance">When compression is enabled, each compressed block includes a checksum of that block for the purpose of detecting disk bitrot and avoiding the propagation of corruption to other replica. This option defines the probability with which those checksums are checked during read. By default they are always checked. Set to 0 to disable checksum checking and to 0.5 for instance to check them every other read</param>
        /// <returns></returns>
        public T WithCompression(string algorithm = "SnappyCompressor", int chunkLength = 64, double crcCheckChance = 1.0)
        {
            var toInclude = new List<IOption>();
            if (algorithm != "SnappyCompressor")
                toInclude.Add(new SimpleOption<string>("sstable_compression", algorithm));
            if (chunkLength != 64)
                toInclude.Add(new SimpleOption<int>("chunk_length_kb", chunkLength));
            if (crcCheckChance != 1.0)
                toInclude.Add(new SimpleOption<double>("crc_check_chance", crcCheckChance, ccc => ccc >= 0 && ccc <= 1.0));

            this.options.Add(new MapOption("compression", toInclude));
            return this as T;
        }

    }
}
