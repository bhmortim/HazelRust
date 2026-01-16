//! Executor service for distributed task execution.
//!
//! This module provides types for submitting tasks to Hazelcast cluster members
//! for remote execution, including scheduled execution with delays and periodic
//! repetition, as well as durable execution with result persistence.

mod durable;
mod scheduled;
mod service;

pub use durable::{DurableExecutorService, DurableFuture};
pub use scheduled::{ScheduledExecutorService, ScheduledFuture, ScheduleType, TimeUnit};
pub use service::ExecutorFuture;

pub use self::{
    Callable, ExecutionCallback, ExecutionTarget, ExecutorService,
    FnExecutionCallback, Runnable, RunnableTask, CallableTask,
};

use std::marker::PhantomData;
use std::sync::Arc;

use hazelcast_core::{Deserializable, HazelcastError, Serializable};

/// A task that computes a result and may throw an exception.
///
/// Implementations must be serializable and have a corresponding
/// server-side implementation registered with the cluster.
pub trait Callable<T>: Serializable + Send + Sync
where
    T: Deserializable,
{
    /// Returns the factory ID for this callable type.
    fn factory_id(&self) -> i32;

    /// Returns the class ID for this callable type.
    fn class_id(&self) -> i32;
}

/// A task that can be executed without returning a result.
///
/// Implementations must be serializable and have a corresponding
/// server-side implementation registered with the cluster.
pub trait Runnable: Serializable + Send + Sync {
    /// Returns the factory ID for this runnable type.
    fn factory_id(&self) -> i32;

    /// Returns the class ID for this runnable type.
    fn class_id(&self) -> i32;
}

/// Callback interface for receiving async execution results.
pub trait ExecutionCallback<T>: Send + Sync {
    /// Called when the task completes successfully.
    fn on_response(&self, result: T);

    /// Called when the task fails with an error.
    fn on_failure(&self, error: HazelcastError);
}

/// Wrapper for runnable tasks with serialization metadata.
#[derive(Debug, Clone)]
pub struct RunnableTask {
    data: Vec<u8>,
    factory_id: i32,
    class_id: i32,
}

impl RunnableTask {
    /// Creates a new runnable task wrapper from a runnable implementation.
    pub fn new<R: Runnable>(task: &R) -> hazelcast_core::Result<Self> {
        let mut data = Vec::new();
        task.serialize(&mut data)?;
        Ok(Self {
            data,
            factory_id: task.factory_id(),
            class_id: task.class_id(),
        })
    }

    /// Returns the serialized task data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns the factory ID.
    pub fn factory_id(&self) -> i32 {
        self.factory_id
    }

    /// Returns the class ID.
    pub fn class_id(&self) -> i32 {
        self.class_id
    }
}

/// Wrapper for callable tasks with serialization metadata.
#[derive(Debug, Clone)]
pub struct CallableTask<T> {
    data: Vec<u8>,
    factory_id: i32,
    class_id: i32,
    _marker: PhantomData<T>,
}

impl<T: Deserializable> CallableTask<T> {
    /// Creates a new callable task wrapper from a callable implementation.
    pub fn new<C: Callable<T>>(task: &C) -> hazelcast_core::Result<Self> {
        let mut data = Vec::new();
        task.serialize(&mut data)?;
        Ok(Self {
            data,
            factory_id: task.factory_id(),
            class_id: task.class_id(),
            _marker: PhantomData,
        })
    }

    /// Returns the serialized task data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns the factory ID.
    pub fn factory_id(&self) -> i32 {
        self.factory_id
    }

    /// Returns the class ID.
    pub fn class_id(&self) -> i32 {
        self.class_id
    }
}

/// Function-based execution callback wrapper.
pub struct FnExecutionCallback<T, F, E>
where
    F: Fn(T) + Send + Sync,
    E: Fn(HazelcastError) + Send + Sync,
{
    on_success: F,
    on_error: E,
    _marker: PhantomData<T>,
}

impl<T, F, E> FnExecutionCallback<T, F, E>
where
    F: Fn(T) + Send + Sync,
    E: Fn(HazelcastError) + Send + Sync,
{
    /// Creates a new callback from success and error handler functions.
    pub fn new(on_success: F, on_error: E) -> Self {
        Self {
            on_success,
            on_error,
            _marker: PhantomData,
        }
    }
}

impl<T, F, E> ExecutionCallback<T> for FnExecutionCallback<T, F, E>
where
    F: Fn(T) + Send + Sync,
    E: Fn(HazelcastError) + Send + Sync,
{
    fn on_response(&self, result: T) {
        (self.on_success)(result);
    }

    fn on_failure(&self, error: HazelcastError) {
        (self.on_error)(error);
    }
}

/// Target for task submission.
#[derive(Debug, Clone)]
pub enum ExecutionTarget {
    /// Submit to any available member.
    Any,
    /// Submit to a specific member by UUID.
    Member(uuid::Uuid),
    /// Submit to the member owning the specified key's partition.
    KeyOwner(Vec<u8>),
}

/// Distributed executor service for submitting tasks to cluster members.
///
/// The executor service allows submitting `Callable` and `Runnable` tasks
/// for execution on remote cluster members.
pub struct ExecutorService {
    name: String,
    connection_manager: Arc<crate::connection::ConnectionManager>,
}

impl ExecutorService {
    /// Creates a new executor service instance.
    pub(crate) fn new(
        name: String,
        connection_manager: Arc<crate::connection::ConnectionManager>,
    ) -> Self {
        Self {
            name,
            connection_manager,
        }
    }

    /// Returns the name of this executor service.
    pub fn name(&self) -> &str {
        &self.name
    }
}
