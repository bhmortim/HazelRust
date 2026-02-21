//! Map interceptor support for intercepting map operations on the server.

use hazelcast_core::Serializable;

/// A map interceptor that intercepts map operations on the server side.
///
/// Map interceptors are executed on the cluster members when map operations occur.
/// They can modify values before they are stored or returned, or perform side effects
/// on remove operations.
///
/// # Implementation
///
/// Implementors must be serializable so they can be sent to cluster members.
/// The interceptor instance is stored on the server and invoked for each operation.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::proxy::MapInterceptor;
/// use hazelcast_core::serialization::ObjectDataOutput;
///
/// struct UpperCaseInterceptor;
///
/// impl MapInterceptor for UpperCaseInterceptor {
///     type Value = String;
///
///     fn intercept_get(&self, value: Option<Self::Value>) -> Option<Self::Value> {
///         value.map(|v| v.to_uppercase())
///     }
///
///     fn intercept_put(&self, _old_value: Option<Self::Value>, new_value: Self::Value) -> Self::Value {
///         new_value.to_uppercase()
///     }
///
///     fn intercept_remove(&self, _removed_value: Option<Self::Value>) {
///         // Optional side effect on remove
///     }
/// }
///
/// impl Serializable for UpperCaseInterceptor {
///     fn serialize(&self, output: &mut ObjectDataOutput) -> hazelcast_core::Result<()> {
///         // Serialize interceptor state
///         Ok(())
///     }
/// }
/// ```
pub trait MapInterceptor: Serializable + Send + Sync {
    /// The value type this interceptor operates on.
    type Value;

    /// Intercepts a get operation.
    ///
    /// Called after a value is retrieved from the map. The returned value
    /// will be given to the caller instead of the original value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value retrieved from the map, or `None` if the key doesn't exist
    ///
    /// # Returns
    ///
    /// The value to return to the caller
    fn intercept_get(&self, value: Option<Self::Value>) -> Option<Self::Value>;

    /// Intercepts a put operation.
    ///
    /// Called before a value is stored in the map. The returned value
    /// will be stored instead of the original new value.
    ///
    /// # Arguments
    ///
    /// * `old_value` - The previous value in the map, or `None` if this is a new entry
    /// * `new_value` - The value being put into the map
    ///
    /// # Returns
    ///
    /// The value to actually store in the map
    fn intercept_put(&self, old_value: Option<Self::Value>, new_value: Self::Value) -> Self::Value;

    /// Intercepts a remove operation.
    ///
    /// Called after a value is removed from the map. This method is for
    /// side effects only and cannot modify the removed value.
    ///
    /// # Arguments
    ///
    /// * `removed_value` - The value that was removed, or `None` if the key didn't exist
    fn intercept_remove(&self, removed_value: Option<Self::Value>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hazelcast_core::serialization::ObjectDataOutput;

    struct TestInterceptor {
        prefix: String,
    }

    impl MapInterceptor for TestInterceptor {
        type Value = String;

        fn intercept_get(&self, value: Option<Self::Value>) -> Option<Self::Value> {
            value.map(|v| format!("{}{}", self.prefix, v))
        }

        fn intercept_put(&self, _old_value: Option<Self::Value>, new_value: Self::Value) -> Self::Value {
            format!("{}{}", self.prefix, new_value)
        }

        fn intercept_remove(&self, _removed_value: Option<Self::Value>) {}
    }

    impl Serializable for TestInterceptor {
        fn serialize<W: hazelcast_core::serialization::DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
            output.write_string(&self.prefix)?;
            Ok(())
        }
    }

    #[test]
    fn test_interceptor_intercept_get() {
        let interceptor = TestInterceptor {
            prefix: "PREFIX_".to_string(),
        };

        let result = interceptor.intercept_get(Some("value".to_string()));
        assert_eq!(result, Some("PREFIX_value".to_string()));

        let result = interceptor.intercept_get(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_interceptor_intercept_put() {
        let interceptor = TestInterceptor {
            prefix: "PREFIX_".to_string(),
        };

        let result = interceptor.intercept_put(None, "new_value".to_string());
        assert_eq!(result, "PREFIX_new_value");

        let result = interceptor.intercept_put(Some("old".to_string()), "new".to_string());
        assert_eq!(result, "PREFIX_new");
    }

    #[test]
    fn test_interceptor_intercept_remove() {
        let interceptor = TestInterceptor {
            prefix: "PREFIX_".to_string(),
        };

        interceptor.intercept_remove(Some("removed".to_string()));
        interceptor.intercept_remove(None);
    }

    #[test]
    fn test_interceptor_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TestInterceptor>();
    }

    #[test]
    fn test_interceptor_serialization() {
        let interceptor = TestInterceptor {
            prefix: "test_".to_string(),
        };

        let mut output = ObjectDataOutput::new();
        interceptor.serialize(&mut output).unwrap();
        let bytes = output.into_bytes();
        assert!(!bytes.is_empty());
    }
}
