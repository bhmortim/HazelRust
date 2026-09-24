/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
