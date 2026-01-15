//! Pipeline definition types for Jet streaming jobs.

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

/// Marker struct for source vertices in pipeline building.
#[derive(Debug, Clone, Copy)]
pub struct Source;

/// Marker struct for sink vertices in pipeline building.
#[derive(Debug, Clone, Copy)]
pub struct Sink;

/// A Jet pipeline definition.
///
/// A pipeline consists of processing vertices connected by edges,
/// forming a directed acyclic graph (DAG) of data transformations.
#[derive(Debug, Clone, Default)]
pub struct Pipeline {
    vertices: Vec<ProcessorVertex>,
    edges: Vec<(usize, usize)>,
}

impl Pipeline {
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
}

/// Builder for constructing Jet pipelines.
#[derive(Debug, Clone, Default)]
pub struct PipelineBuilder {
    vertices: Vec<ProcessorVertex>,
    edges: Vec<(usize, usize)>,
}

impl PipelineBuilder {
    /// Creates a new pipeline builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a source vertex that reads from the named data structure.
    pub fn read_from(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("source:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a map transformation vertex.
    pub fn map(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("map:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a filter transformation vertex.
    pub fn filter(mut self, name: impl Into<String>) -> Self {
        let vertex = ProcessorVertex::new(format!("filter:{}", name.into()));
        self.vertices.push(vertex);
        self.connect_to_previous();
        self
    }

    /// Adds a sink vertex that writes to the named data structure.
    pub fn write_to(mut self, name: impl Into<String>) -> Self {
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
            .read_from("source")
            .write_to("sink")
            .build();

        assert_eq!(pipeline.vertex_count(), 2);
        assert_eq!(pipeline.edge_count(), 1);
        assert_eq!(pipeline.edges()[0], (0, 1));
    }

    #[test]
    fn test_pipeline_builder_full() {
        let pipeline = Pipeline::builder()
            .read_from("source")
            .map("transform")
            .filter("filter")
            .write_to("sink")
            .build();

        assert_eq!(pipeline.vertex_count(), 4);
        assert_eq!(pipeline.edge_count(), 3);
        assert_eq!(pipeline.edges(), &[(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn test_pipeline_vertex_names() {
        let pipeline = Pipeline::builder()
            .read_from("my-source")
            .map("my-map")
            .filter("my-filter")
            .write_to("my-sink")
            .build();

        assert_eq!(pipeline.vertices()[0].name(), "source:my-source");
        assert_eq!(pipeline.vertices()[1].name(), "map:my-map");
        assert_eq!(pipeline.vertices()[2].name(), "filter:my-filter");
        assert_eq!(pipeline.vertices()[3].name(), "sink:my-sink");
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
    fn test_source_sink_markers() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<Source>();
        assert_copy::<Sink>();
    }
}
