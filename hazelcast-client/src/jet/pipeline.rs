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

/// A source that reads from files matching a glob pattern.
#[derive(Debug, Clone)]
pub struct FileSource {
    directory: String,
    glob_pattern: String,
}

impl FileSource {
    /// Creates a new file source with the given directory and glob pattern.
    pub fn new(directory: impl Into<String>, glob_pattern: impl Into<String>) -> Self {
        Self {
            directory: directory.into(),
            glob_pattern: glob_pattern.into(),
        }
    }

    /// Returns the directory path.
    pub fn directory(&self) -> &str {
        &self.directory
    }

    /// Returns the glob pattern.
    pub fn glob_pattern(&self) -> &str {
        &self.glob_pattern
    }
}

impl Source for FileSource {
    fn source_type(&self) -> &str {
        "files"
    }

    fn name(&self) -> &str {
        &self.directory
    }

    fn vertex_name(&self) -> String {
        format!("source:files:{}:{}", self.directory, self.glob_pattern)
    }
}

/// A source that reads from a database via JDBC.
#[derive(Debug, Clone)]
pub struct JdbcSource {
    connection_string: String,
    query: String,
}

impl JdbcSource {
    /// Creates a new JDBC source with the given connection string and query.
    pub fn new(connection_string: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            query: query.into(),
        }
    }

    /// Returns the JDBC connection string.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Returns the SQL query.
    pub fn query(&self) -> &str {
        &self.query
    }
}

impl Source for JdbcSource {
    fn source_type(&self) -> &str {
        "jdbc"
    }

    fn name(&self) -> &str {
        &self.connection_string
    }

    fn vertex_name(&self) -> String {
        format!("source:jdbc:{}", self.connection_string)
    }
}

/// File format for file sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileFormat {
    /// Plain text format, one item per line.
    #[default]
    Text,
    /// JSON format.
    Json,
    /// CSV format.
    Csv,
}

impl FileFormat {
    /// Returns the format name as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            FileFormat::Text => "text",
            FileFormat::Json => "json",
            FileFormat::Csv => "csv",
        }
    }
}

/// A sink that writes to files in a directory.
#[derive(Debug, Clone)]
pub struct FileSink {
    directory: String,
    format: FileFormat,
}

impl FileSink {
    /// Creates a new file sink with the given directory and format.
    pub fn new(directory: impl Into<String>, format: FileFormat) -> Self {
        Self {
            directory: directory.into(),
            format,
        }
    }

    /// Returns the directory path.
    pub fn directory(&self) -> &str {
        &self.directory
    }

    /// Returns the file format.
    pub fn format(&self) -> FileFormat {
        self.format
    }
}

impl Sink for FileSink {
    fn sink_type(&self) -> &str {
        "files"
    }

    fn name(&self) -> &str {
        &self.directory
    }

    fn vertex_name(&self) -> String {
        format!("sink:files:{}:{}", self.directory, self.format.as_str())
    }
}

/// A sink that collects results in-memory for observation.
#[derive(Debug, Clone)]
pub struct ObservableSink {
    name: String,
}

impl ObservableSink {
    /// Creates a new observable sink with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Sink for ObservableSink {
    fn sink_type(&self) -> &str {
        "observable"
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Factory for creating pipeline sources.
pub struct Sources;

impl Sources {
    /// Creates a source that reads from an IMap.
    pub fn map(name: impl Into<String>) -> MapSource {
        MapSource::new(name)
    }

    /// Creates a source that reads from an IList.
    pub fn list(name: impl Into<String>) -> ListSource {
        ListSource::new(name)
    }

    /// Creates a source that reads from files matching a glob pattern.
    pub fn files(directory: impl Into<String>, glob_pattern: impl Into<String>) -> FileSource {
        FileSource::new(directory, glob_pattern)
    }

    /// Creates a source that reads from a database via JDBC.
    pub fn jdbc(connection_string: impl Into<String>, query: impl Into<String>) -> JdbcSource {
        JdbcSource::new(connection_string, query)
    }
}

/// Factory for creating pipeline sinks.
pub struct Sinks;

impl Sinks {
    /// Creates a sink that writes to an IMap.
    pub fn map(name: impl Into<String>) -> MapSink {
        MapSink::new(name)
    }

    /// Creates a sink that writes to an IList.
    pub fn list(name: impl Into<String>) -> ListSink {
        ListSink::new(name)
    }

    /// Creates a sink that writes to files in a directory.
    pub fn files(directory: impl Into<String>, format: FileFormat) -> FileSink {
        FileSink::new(directory, format)
    }

    /// Creates an observable sink for in-memory result collection.
    pub fn observable(name: impl Into<String>) -> ObservableSink {
        ObservableSink::new(name)
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

/// Creates a source that reads from files matching a glob pattern.
pub fn file_source(directory: impl Into<String>, glob_pattern: impl Into<String>) -> FileSource {
    FileSource::new(directory, glob_pattern)
}

/// Creates a source that reads from a database via JDBC.
pub fn jdbc_source(connection_string: impl Into<String>, query: impl Into<String>) -> JdbcSource {
    JdbcSource::new(connection_string, query)
}

/// Creates a sink that writes to files in a directory.
pub fn file_sink(directory: impl Into<String>, format: FileFormat) -> FileSink {
    FileSink::new(directory, format)
}

/// Creates an observable sink for in-memory result collection.
pub fn observable_sink(name: impl Into<String>) -> ObservableSink {
    ObservableSink::new(name)
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

    #[test]
    fn test_file_source() {
        let source = file_source("/data/input", "*.csv");
        assert_eq!(source.source_type(), "files");
        assert_eq!(source.directory(), "/data/input");
        assert_eq!(source.glob_pattern(), "*.csv");
        assert_eq!(source.vertex_name(), "source:files:/data/input:*.csv");
    }

    #[test]
    fn test_jdbc_source() {
        let source = jdbc_source("jdbc:postgresql://localhost/db", "SELECT * FROM users");
        assert_eq!(source.source_type(), "jdbc");
        assert_eq!(source.connection_string(), "jdbc:postgresql://localhost/db");
        assert_eq!(source.query(), "SELECT * FROM users");
        assert_eq!(
            source.vertex_name(),
            "source:jdbc:jdbc:postgresql://localhost/db"
        );
    }

    #[test]
    fn test_file_sink() {
        let sink = file_sink("/data/output", FileFormat::Json);
        assert_eq!(sink.sink_type(), "files");
        assert_eq!(sink.directory(), "/data/output");
        assert_eq!(sink.format(), FileFormat::Json);
        assert_eq!(sink.vertex_name(), "sink:files:/data/output:json");
    }

    #[test]
    fn test_file_format() {
        assert_eq!(FileFormat::Text.as_str(), "text");
        assert_eq!(FileFormat::Json.as_str(), "json");
        assert_eq!(FileFormat::Csv.as_str(), "csv");
        assert_eq!(FileFormat::default(), FileFormat::Text);
    }

    #[test]
    fn test_observable_sink() {
        let sink = observable_sink("results");
        assert_eq!(sink.sink_type(), "observable");
        assert_eq!(sink.name(), "results");
        assert_eq!(sink.vertex_name(), "sink:observable:results");
    }

    #[test]
    fn test_sources_factory() {
        let map = Sources::map("my-map");
        assert_eq!(map.source_type(), "map");

        let list = Sources::list("my-list");
        assert_eq!(list.source_type(), "list");

        let files = Sources::files("/data", "*.txt");
        assert_eq!(files.source_type(), "files");

        let jdbc = Sources::jdbc("jdbc:mysql://host/db", "SELECT 1");
        assert_eq!(jdbc.source_type(), "jdbc");
    }

    #[test]
    fn test_sinks_factory() {
        let map = Sinks::map("my-map");
        assert_eq!(map.sink_type(), "map");

        let list = Sinks::list("my-list");
        assert_eq!(list.sink_type(), "list");

        let files = Sinks::files("/output", FileFormat::Csv);
        assert_eq!(files.sink_type(), "files");

        let observable = Sinks::observable("results");
        assert_eq!(observable.sink_type(), "observable");
    }

    #[test]
    fn test_pipeline_with_file_connectors() {
        let pipeline = Pipeline::builder()
            .read_from(Sources::files("/input", "*.json"))
            .map("parse")
            .filter("validate")
            .write_to(Sinks::files("/output", FileFormat::Json))
            .build();

        assert_eq!(pipeline.vertex_count(), 4);
        assert_eq!(pipeline.sources().len(), 1);
        assert_eq!(pipeline.sinks().len(), 1);
        assert_eq!(
            pipeline.vertices()[0].name(),
            "source:files:/input:*.json"
        );
        assert_eq!(
            pipeline.vertices()[3].name(),
            "sink:files:/output:json"
        );
    }

    #[test]
    fn test_pipeline_with_jdbc_source() {
        let pipeline = Pipeline::builder()
            .read_from(Sources::jdbc(
                "jdbc:postgresql://localhost:5432/mydb",
                "SELECT id, name FROM customers",
            ))
            .map("transform")
            .write_to(Sinks::map("customer-cache"))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.sources().len(), 1);
        assert_eq!(pipeline.sources()[0].source_type(), "jdbc");
    }

    #[test]
    fn test_pipeline_with_observable_sink() {
        let pipeline = Pipeline::builder()
            .read_from(Sources::map("input"))
            .aggregate("count")
            .write_to(Sinks::observable("query-results"))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.sinks().len(), 1);
        assert_eq!(pipeline.sinks()[0].sink_type(), "observable");
        assert_eq!(
            pipeline.vertices()[2].name(),
            "sink:observable:query-results"
        );
    }

    #[test]
    fn test_new_connectors_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileSource>();
        assert_send_sync::<JdbcSource>();
        assert_send_sync::<FileSink>();
        assert_send_sync::<ObservableSink>();
    }
}
