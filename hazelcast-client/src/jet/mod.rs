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

//! Jet streaming APIs for Hazelcast.

mod config;
#[cfg(feature = "kafka")]
pub mod connectors;
mod job;
mod pipeline;
mod service;
mod status;

pub use config::{JobConfig, JobConfigBuilder, ProcessingGuarantee};
pub use job::Job;
pub use pipeline::{
    file_sink, file_source, jdbc_source, list_sink, list_source, map_sink, map_source,
    observable_sink, AggregateOperation, FileFormat, FileSink, FileSource, JdbcSource,
    JoinCondition, JoinedStream, ListSink, ListSource, MapSink, MapSource, ObservableSink,
    Pipeline, PipelineBuilder, ProcessorVertex, Sink, Sinks, Source, Sources, WindowDefinition,
};
pub use service::JetService;
pub use status::{JobMetrics, JobStatus};

#[cfg(feature = "kafka")]
pub use connectors::kafka::{
    kafka_sink, kafka_source, Acks, AutoOffsetReset, CompressionType, IsolationLevel, KafkaSink,
    KafkaSinkConfig, KafkaSinkConfigBuilder, KafkaSource, KafkaSourceConfig,
    KafkaSourceConfigBuilder,
};
