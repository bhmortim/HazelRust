package bench;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/** Mirror of the JSON produced by bench/gen_manifest.py (same schema as the Rust manifest.rs). */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Manifest {
    public Meta meta;
    public List<Cell> cells;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Meta {
        public int schema_version;
        public String tier;
        public long seed;
        public double zipf_theta = 0.99;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Cell {
        public String id;
        public String suite;
        public String structure;
        public String op;
        public String variant = "default";
        public String key_type;
        public String value_kind;
        public int value_size;
        public long working_set;
        public int concurrency;
        public String load_model;
        public String rate_group;
        public Double rate_frac;
        public Double target_rate;
        public Integer batch_size;
        public String distribution;
        public double zipf_theta = 0.99;
        public long warmup_s;
        public long measure_s;
        public int forks;
        public int trials;
        public long min_ops;
        public Map<String, Double> mix;
    }

    public Cell find(String cellId) {
        for (Cell c : cells) if (c.id.equals(cellId)) return c;
        return null;
    }
}
