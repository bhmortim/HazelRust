//! Pipeline definition types for Jet streaming jobs.

use std::sync::Arc;

/// Default local parallelism value (-1 means use default).
const DEFAULT_LOCAL_PARALLELISM: i32 = -1;

/// A processing vertex in a Jet pipeline.
#[derive(Debug, Clone)]
pub struct ProcessorVertex {
    name: String,
    local_parallelism: i32,
}

impl ProcessorVertex {
    /// Creates a new processor vertex with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            local_parallelism: DEFAULT_LOCAL_PARALLELISM,
        }
    }

    /// Creates a new processor vertex with custom local parallelism.
    pub fn with_parallelism(name: impl Into<String>, parallelism: i32) -> Self {
        Self {
            name: name.into(),
            local_parallelism: parallelism,
        }
    }

    /// Returns the vertex name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the local parallelism.
    ///
    /// A value of -1 indicates the default parallelism should be used.
    pub fn local_parallelism(&self) -> i32 {
        self.local_parallelism
    }
}

/// Trait for pipeline sources that produce data.
pub trait Source: Send + Sync + std::fmt::Debug {
    /// Returns the source type identifier (e.g., "map", "list").
    fn source_type(&self) -> &str;

    /// Returns the name of the data structure to read from.
    fn name(&self) -> &str;

    /// Returns the full vertex name for this source.
    fn vertex_name(&self) -> String {
        format!("source:{}:{}", self.source_type(), self.name())
    }
}

/// Trait for pipeline sinks that consume data.
pub trait Sink: Send + Sync + std::fmt::Debug {
    /// Returns the sink type identifier (e.g., "map", "list").
    fn sink_type(&self) -> &str;

    /// Returns the name of the data structure to write to.
    fn name(&self) -> &str;

    /// Returns the full vertex name for this sink.
    fn vertex_name(&self) -> String {
        format!("sink:{}:{}", self.sink_type(), self.name())
    }
}

/// A source that reads from an IMap.
#[derive(Debug, Clone)]
pub struct MapSource {
    name: String,
}

impl MapSource {
    /// Creates a new map source with the given map name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Source for MapSource {
    fn source_type(&self) -> &str {
        "map"
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A source that reads from an IList.
#[derive(Debug, Clone)]
pub struct ListSource {
    name: String,
}

impl ListSource {
    /// Creates a new list source with the given list name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Source for ListSource {
    fn source_type(&self) -> &str {
        "list"
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A sink that writes to an IMap.
#[derive(Debug, Clone)]
pub struct MapSink {
    name: String,
}

impl MapSink {
    /// Creates a new map sink with the given map name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Sink for MapSink {
    fn sink_type(&self) -> &str {
        "map"
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A sink that writes to an IList.
#[derive(Debug, Clone)]
pub struct ListSink {
    name: String,
}

impl ListSink {
    /// Creates a new list sink with the given list name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Sink for ListSink {
    fn sink_type(&self) -> &str {
        "list"
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Creates a source that reads from an IMap with the given name.
pub fn map_source(name: impl Into<String>) -> MapSource {
    MapSource::new(name)
}

/// Creates a source that reads from an IList with the given name.
pub fn list_source(name: impl Into<String>) -> ListSource {
    ListSource::new(name)
}

/// Creates a sink that writes to an IMap with the given name.
pub fn map_sink(name: impl Into<String>) -> MapSink {
    MapSink::new(name)
}

/// Creates a sink that writes to an IList with the given name.
pub fn list_sink(name: impl Into<String>) -> ListSink {
    ListSink::new(name)
}

/// A Jet pipeline definition.
///
/// A pipeline consists of processing vertices connected by edges,
/// forming a directed acyclic graph (DAG) of data transformations.
#[derive(Debug, Clone, Default)]
pub struct Pipeline {
    vertices: Vec<ProcessorVertex>,
    edges: Vec<(usize, usize)>,
    sources: Vec<Arc<dyn Source>>,
    sinks: Vec<Arc<dyn Sink>>,
}

impl Pipeline {
    /// Creates a new empty pipeline.
    pub fn create() -> PipelineBuilder {
        PipelineBuilder::new()
    }

    /// Creates a new pipeline builder.
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::new()
    }

    /// Returns the list of processing vertices.
    pub fn vertices(&self) -> &[ProcessorVertex] {
        &self.vertices
    }

    /// Returns the edges between vertices.
    ///
    /// Each edge is represented as a tuple of (from_index, to_index).
    pub fn edges(&self) -> &[(usize, usize)] {
        &self.edges
    }

    /// Returns the number of vertices in the pipeline.
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Returns the number of edges in the pipeline.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns true if the pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty()
    }

    /// Returns the sources used in this pipeline.
    pub fn sources(&self) -> &[Arc<dyn Source>] {
        &self.sources
    }

    /// Returns the sinks used in this pipeline.
    pub fn sinks(&self) -> &[Arc<dyn Sink>] {
        &self.sinks
    }
}

/// Builder for constructing Jet pipelines.
#[derive(Debug, Default)]
pub struct PipelineBuilder {
    vertices: Vec<ProcessorVertex>,
    edges: Vec<(usize, usize)>,
    sources: Vec<Arc<dyn Source>>,
    sinks: Vec<Arc<dyn Sink>>,
}

impl Clone for PipelineBuilder {
    fn clone(&self) -> Self {
        Self {
            vertices: self.vertices.clone(),
            edges: self.edges.clone(),
            sources: self.sources.clone(),
            sinks: self.sinks.clone(),
        }
    }
}

impl PipelineBuilder {
    /// Creates a new pipeline builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a source vertex that reads from the given source.
    pub fn read_from<S: Source + 'static>(mut self, source: S) -> Self {
        let vertex = ProcessorVertex::new(source.vertex_name());
        self.vertices.push(vertex);
        self.sources.push(Arc::new(source));
        self.connect_to_previous();
        self
    }

    /// Adds a source vertex that reads from the named data structure.
    pub fn read_from_name(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("source:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a map transformation vertex that applies a transformation function.
    pub fn map(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("map:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a filter transformation vertex that filters items based on a predicate.
    pub fn filter(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("filter:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a flat_map transformation vertex that maps each item to zero or more items.
    pub fn flat_map(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("flat_map:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a group_by vertex that groups items by a key extractor.
    pub fn group_by(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("group_by:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds an aggregate vertex that performs aggregation operations.
    pub fn aggregate(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("aggregate:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a sink vertex that writes to the given sink.
    pub fn write_to<S: Sink + 'static>(mut self, sink: S) -> Self {
        let vertex = ProcessorVertex::new(sink.vertex_name());
        self.vertices.push(vertex);
        self.sinks.push(Arc::new(sink));
        self.connect_to_previous();
        self
    }

    /// Adds a sink vertex that writes to the named data structure.
    pub fn write_to_name(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("sink:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a custom vertex with the given name.
    pub fn vertex(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(name);
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a custom vertex with specific local parallelism.
    pub fn vertex_with_parallelism(mut self, name: impl Into<String>, parallelism: i32) -> Self {
        let vertex = ProcessorVertex::with_parallelism(name, parallelism);
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Connects the last added vertex to the previous one.
    fn connect_to_previous(&mut self) {
        let len = self.vertices.len();
        if len > 1 {
            self.edges.push((len - 2, len - 1));
        }
    }

    /// Builds the pipeline.
    pub fn build(self) -> Pipeline {
        Pipeline {
            vertices: self.vertices,
            edges: self.edges,
            sources: self.sources,
            sinks: self.sinks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_vertex_new() {
        let vertex = ProcessorVertex::new("test");
        assert_eq!(vertex.name(), "test");
        assert_eq!(vertex.local_parallelism(), -1);
    }

    #[test]
    fn test_processor_vertex_with_parallelism() {
        let vertex = ProcessorVertex::with_parallelism("test", 4);
        assert_eq!(vertex.name(), "test");
        assert_eq!(vertex.local_parallelism(), 4);
    }

    #[test]
    fn test_pipeline_builder_empty() {
        let pipeline = Pipeline::builder().build();
        assert!(pipeline.is_empty());
        assert!(pipeline.vertices().is_empty());
        assert!(pipeline.edges().is_empty());
        assert_eq!(pipeline.vertex_count(), 0);
        assert_eq!(pipeline.edge_count(), 0);
    }

    #[test]
    fn test_pipeline_builder_simple() {
        let pipeline = Pipeline::builder()
            .read_from(map_source("source"))
            .write_to(map_sink("sink"))
            .build();

        assert_eq!(pipeline.vertex_count(), 2);
        assert_eq!(pipeline.edge_count(), 1);
        assert_eq!(pipeline.edges()[0], (0, 1));
        assert_eq!(pipeline.sources().len(), 1);
        assert_eq!(pipeline.sinks().len(), 1);
    }

    #[test]
    fn test_pipeline_builder_full() {
        let pipeline = Pipeline::builder()
            .read_from(map_source("source"))
            .map("transform")
            .filter("filter")
            .write_to(map_sink("sink"))
            .build();

        assert_eq!(pipeline.vertex_count(), 4);
        assert_eq!(pipeline.edge_count(), 3);
        assert_eq!(pipeline.edges(), &[(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn test_pipeline_vertex_names() {
        let pipeline = Pipeline::builder()
            .read_from(map_source("my-source"))
            .map("my-map")
            .filter("my-filter")
            .write_to(map_sink("my-sink"))
            .build();

        assert_eq!(pipeline.vertices()[0].name(), "source:map:my-source");
        assert_eq!(pipeline.vertices()[1].name(), "map:my-map");
        assert_eq!(pipeline.vertices()[2].name(), "filter:my-filter");
        assert_eq!(pipeline.vertices()[3].name(), "sink:map:my-sink");
    }

    #[test]
    fn test_pipeline_custom_vertex() {
        let pipeline = Pipeline::builder()
            .vertex("custom-processor")
            .vertex_with_parallelism("parallel-processor", 8)
            .build();

        assert_eq!(pipeline.vertex_count(), 2);
        assert_eq!(pipeline.vertices()[0].name(), "custom-processor");
        assert_eq!(pipeline.vertices()[0].local_parallelism(), -1);
        assert_eq!(pipeline.vertices()[1].name(), "parallel-processor");
        assert_eq!(pipeline.vertices()[1].local_parallelism(), 8);
    }

    #[test]
    fn test_pipeline_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Pipeline>();
    }

    #[test]
    fn test_pipeline_builder_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PipelineBuilder>();
    }

    #[test]
    fn test_map_source() {
        let source = map_source("test-map");
        assert_eq!(source.source_type(), "map");
        assert_eq!(source.name(), "test-map");
        assert_eq!(source.vertex_name(), "source:map:test-map");
    }

    #[test]
    fn test_list_source() {
        let source = list_source("test-list");
        assert_eq!(source.source_type(), "list");
        assert_eq!(source.name(), "test-list");
        assert_eq!(source.vertex_name(), "source:list:test-list");
    }

    #[test]
    fn test_map_sink() {
        let sink = map_sink("test-map");
        assert_eq!(sink.sink_type(), "map");
        assert_eq!(sink.name(), "test-map");
        assert_eq!(sink.vertex_name(), "sink:map:test-map");
    }

    #[test]
    fn test_list_sink() {
        let sink = list_sink("test-list");
        assert_eq!(sink.sink_type(), "list");
        assert_eq!(sink.name(), "test-list");
        assert_eq!(sink.vertex_name(), "sink:list:test-list");
    }

    #[test]
    fn test_pipeline_create() {
        let pipeline = Pipeline::create()
            .read_from(list_source("input"))
            .map("transform")
            .write_to(list_sink("output"))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.sources().len(), 1);
        assert_eq!(pipeline.sinks().len(), 1);
    }

    #[test]
    fn test_pipeline_flat_map() {
        let pipeline = Pipeline::builder()
            .read_from(map_source("input"))
            .flat_map("splitter")
            .write_to(list_sink("output"))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.vertices()[1].name(), "flat_map:splitter");
    }

    #[test]
    fn test_pipeline_group_by() {
        let pipeline = Pipeline::builder()
            .read_from(map_source("input"))
            .group_by("key_extractor")
            .write_to(map_sink("output"))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.vertices()[1].name(), "group_by:key_extractor");
    }

    #[test]
    fn test_pipeline_aggregate() {
        let pipeline = Pipeline::builder()
            .read_from(map_source("input"))
            .group_by("key")
            .aggregate("sum")
            .write_to(map_sink("output"))
            .build();

        assert_eq!(pipeline.vertex_count(), 4);
        assert_eq!(pipeline.vertices()[2].name(), "aggregate:sum");
    }

    #[test]
    fn test_pipeline_with_name_methods() {
        let pipeline = Pipeline::builder()
            .read_from_name("legacy-source")
            .map("transform")
            .write_to_name("legacy-sink")
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.vertices()[0].name(), "source:legacy-source");
        assert_eq!(pipeline.vertices()[2].name(), "sink:legacy-sink");
    }

    #[test]
    fn test_source_trait_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MapSource>();
        assert_send_sync::<ListSource>();
    }

    #[test]
    fn test_sink_trait_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MapSink>();
        assert_send_sync::<ListSink>();
    }
}
