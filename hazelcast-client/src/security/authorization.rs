//! Role-Based Access Control (RBAC) for Hazelcast operations.

use std::collections::HashSet;

/// Permissions for Hazelcast data structure operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Read data from structures (get, contains, size, etc.)
    Read,
    /// Write/update data (put, set, replace, etc.)
    Write,
    /// Create new data structures.
    Create,
    /// Destroy/delete data structures.
    Destroy,
    /// Put operations specifically.
    Put,
    /// Remove items from structures.
    Remove,
    /// Add event listeners.
    Listen,
    /// Locking operations.
    Lock,
    /// Create indexes.
    Index,
    /// Add interceptors.
    Intercept,
    /// All permissions (wildcard).
    All,
}

impl Permission {
    /// Returns true if this permission implies the other permission.
    pub fn implies(&self, other: Permission) -> bool {
        match self {
            Permission::All => true,
            Permission::Write => matches!(other, Permission::Put | Permission::Remove),
            _ => *self == other,
        }
    }
}

/// Resource type for permission scoping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    /// Map data structure.
    Map,
    /// Queue data structure.
    Queue,
    /// Topic data structure.
    Topic,
    /// List data structure.
    List,
    /// Set data structure.
    Set,
    /// MultiMap data structure.
    MultiMap,
    /// ReplicatedMap data structure.
    ReplicatedMap,
    /// RingBuffer data structure.
    RingBuffer,
    /// Cache data structure.
    Cache,
    /// All resource types (wildcard).
    All,
}

impl ResourceType {
    /// Returns true if this resource type matches the other.
    pub fn matches(&self, other: ResourceType) -> bool {
        *self == ResourceType::All || other == ResourceType::All || *self == other
    }
}

/// A permission grant for a specific resource pattern.
#[derive(Debug, Clone)]
pub struct PermissionGrant {
    resource_type: ResourceType,
    resource_pattern: String,
    permissions: HashSet<Permission>,
}

impl PermissionGrant {
    /// Creates a new permission grant for all resources of the given type.
    pub fn new(resource_type: ResourceType) -> Self {
        Self {
            resource_type,
            resource_pattern: "*".to_string(),
            permissions: HashSet::new(),
        }
    }

    /// Sets the resource name pattern (supports `*` wildcard).
    pub fn with_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.resource_pattern = pattern.into();
        self
    }

    /// Adds a permission to this grant.
    pub fn with_permission(mut self, permission: Permission) -> Self {
        self.permissions.insert(permission);
        self
    }

    /// Adds multiple permissions to this grant.
    pub fn with_permissions(mut self, permissions: impl IntoIterator<Item = Permission>) -> Self {
        self.permissions.extend(permissions);
        self
    }

    /// Grants all permissions.
    pub fn with_all_permissions(mut self) -> Self {
        self.permissions.insert(Permission::All);
        self
    }

    /// Returns the resource type.
    pub fn resource_type(&self) -> ResourceType {
        self.resource_type
    }

    /// Returns the resource pattern.
    pub fn resource_pattern(&self) -> &str {
        &self.resource_pattern
    }

    /// Returns the granted permissions.
    pub fn permissions(&self) -> &HashSet<Permission> {
        &self.permissions
    }

    /// Checks if this grant allows the specified permission on the resource.
    pub fn allows(&self, permission: Permission, resource_type: ResourceType, resource_name: &str) -> bool {
        if !self.resource_type.matches(resource_type) {
            return false;
        }

        if !self.matches_pattern(resource_name) {
            return false;
        }

        self.permissions.iter().any(|p| p.implies(permission))
    }

    fn matches_pattern(&self, resource_name: &str) -> bool {
        if self.resource_pattern == "*" {
            return true;
        }

        if self.resource_pattern.ends_with('*') {
            let prefix = &self.resource_pattern[..self.resource_pattern.len() - 1];
            return resource_name.starts_with(prefix);
        }

        if self.resource_pattern.starts_with('*') {
            let suffix = &self.resource_pattern[1..];
            return resource_name.ends_with(suffix);
        }

        self.resource_pattern == resource_name
    }
}

/// A role with a collection of permission grants.
#[derive(Debug, Clone)]
pub struct Role {
    name: String,
    grants: Vec<PermissionGrant>,
}

impl Role {
    /// Creates a new role with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            grants: Vec::new(),
        }
    }

    /// Adds a permission grant to this role.
    pub fn with_grant(mut self, grant: PermissionGrant) -> Self {
        self.grants.push(grant);
        self
    }

    /// Adds multiple permission grants to this role.
    pub fn with_grants(mut self, grants: impl IntoIterator<Item = PermissionGrant>) -> Self {
        self.grants.extend(grants);
        self
    }

    /// Returns the role name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the permission grants.
    pub fn grants(&self) -> &[PermissionGrant] {
        &self.grants
    }

    /// Checks if this role allows the specified permission.
    pub fn allows(&self, permission: Permission, resource_type: ResourceType, resource_name: &str) -> bool {
        self.grants.iter().any(|g| g.allows(permission, resource_type, resource_name))
    }
}

/// Predefined roles for common use cases.
impl Role {
    /// Creates a read-only role for all resources.
    pub fn read_only(name: impl Into<String>) -> Self {
        Self::new(name).with_grant(
            PermissionGrant::new(ResourceType::All)
                .with_permission(Permission::Read)
                .with_permission(Permission::Listen),
        )
    }

    /// Creates a read-write role for all resources.
    pub fn read_write(name: impl Into<String>) -> Self {
        Self::new(name).with_grant(
            PermissionGrant::new(ResourceType::All)
                .with_permission(Permission::Read)
                .with_permission(Permission::Write)
                .with_permission(Permission::Put)
                .with_permission(Permission::Remove)
                .with_permission(Permission::Listen),
        )
    }

    /// Creates an admin role with all permissions.
    pub fn admin(name: impl Into<String>) -> Self {
        Self::new(name).with_grant(
            PermissionGrant::new(ResourceType::All).with_all_permissions(),
        )
    }
}

/// Error returned when a permission check fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionDenied {
    /// The permission that was denied.
    pub permission: Permission,
    /// The resource type.
    pub resource_type: ResourceType,
    /// The resource name.
    pub resource_name: String,
    /// Error message.
    pub message: String,
}

impl std::fmt::Display for PermissionDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "permission denied: {:?} on {:?} '{}' - {}",
            self.permission, self.resource_type, self.resource_name, self.message
        )
    }
}

impl std::error::Error for PermissionDenied {}

/// Authorization context for checking permissions against assigned roles.
#[derive(Debug, Clone, Default)]
pub struct AuthorizationContext {
    roles: Vec<Role>,
    enabled: bool,
}

impl AuthorizationContext {
    /// Creates a new authorization context with no roles.
    pub fn new() -> Self {
        Self {
            roles: Vec::new(),
            enabled: true,
        }
    }

    /// Creates a disabled authorization context (all checks pass).
    pub fn disabled() -> Self {
        Self {
            roles: Vec::new(),
            enabled: false,
        }
    }

    /// Adds a role to this context.
    pub fn with_role(mut self, role: Role) -> Self {
        self.roles.push(role);
        self
    }

    /// Adds multiple roles to this context.
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = Role>) -> Self {
        self.roles.extend(roles);
        self
    }

    /// Enables or disables authorization checks.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Returns whether authorization is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the assigned roles.
    pub fn roles(&self) -> &[Role] {
        &self.roles
    }

    /// Checks if the context has permission for the specified operation.
    ///
    /// Returns `Ok(())` if permission is granted, or `Err(PermissionDenied)` if denied.
    pub fn check_permission(
        &self,
        permission: Permission,
        resource_type: ResourceType,
        resource_name: &str,
    ) -> Result<(), PermissionDenied> {
        if !self.enabled {
            return Ok(());
        }

        if self.roles.is_empty() {
            return Err(PermissionDenied {
                permission,
                resource_type,
                resource_name: resource_name.to_string(),
                message: "no roles assigned".to_string(),
            });
        }

        if self.roles.iter().any(|r| r.allows(permission, resource_type, resource_name)) {
            Ok(())
        } else {
            Err(PermissionDenied {
                permission,
                resource_type,
                resource_name: resource_name.to_string(),
                message: format!(
                    "none of the assigned roles grant {:?} permission",
                    permission
                ),
            })
        }
    }

    /// Checks read permission on a map.
    pub fn check_map_read(&self, map_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Read, ResourceType::Map, map_name)
    }

    /// Checks write permission on a map.
    pub fn check_map_write(&self, map_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Write, ResourceType::Map, map_name)
    }

    /// Checks put permission on a map.
    pub fn check_map_put(&self, map_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Put, ResourceType::Map, map_name)
    }

    /// Checks remove permission on a map.
    pub fn check_map_remove(&self, map_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Remove, ResourceType::Map, map_name)
    }

    /// Checks create permission on a map.
    pub fn check_map_create(&self, map_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Create, ResourceType::Map, map_name)
    }

    /// Checks destroy permission on a map.
    pub fn check_map_destroy(&self, map_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Destroy, ResourceType::Map, map_name)
    }

    /// Checks read permission on a queue.
    pub fn check_queue_read(&self, queue_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Read, ResourceType::Queue, queue_name)
    }

    /// Checks write permission on a queue.
    pub fn check_queue_write(&self, queue_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Write, ResourceType::Queue, queue_name)
    }

    /// Checks put permission on a queue.
    pub fn check_queue_put(&self, queue_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Put, ResourceType::Queue, queue_name)
    }

    /// Checks remove permission on a queue.
    pub fn check_queue_remove(&self, queue_name: &str) -> Result<(), PermissionDenied> {
        self.check_permission(Permission::Remove, ResourceType::Queue, queue_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_implies_self() {
        assert!(Permission::Read.implies(Permission::Read));
        assert!(Permission::Write.implies(Permission::Write));
        assert!(Permission::Create.implies(Permission::Create));
    }

    #[test]
    fn test_permission_all_implies_everything() {
        assert!(Permission::All.implies(Permission::Read));
        assert!(Permission::All.implies(Permission::Write));
        assert!(Permission::All.implies(Permission::Create));
        assert!(Permission::All.implies(Permission::Destroy));
        assert!(Permission::All.implies(Permission::Put));
        assert!(Permission::All.implies(Permission::Remove));
        assert!(Permission::All.implies(Permission::Listen));
        assert!(Permission::All.implies(Permission::Lock));
        assert!(Permission::All.implies(Permission::Index));
        assert!(Permission::All.implies(Permission::Intercept));
        assert!(Permission::All.implies(Permission::All));
    }

    #[test]
    fn test_permission_write_implies_put_and_remove() {
        assert!(Permission::Write.implies(Permission::Put));
        assert!(Permission::Write.implies(Permission::Remove));
        assert!(!Permission::Write.implies(Permission::Read));
        assert!(!Permission::Write.implies(Permission::Create));
    }

    #[test]
    fn test_permission_does_not_imply_others() {
        assert!(!Permission::Read.implies(Permission::Write));
        assert!(!Permission::Read.implies(Permission::Create));
        assert!(!Permission::Put.implies(Permission::Remove));
    }

    #[test]
    fn test_resource_type_matches_self() {
        assert!(ResourceType::Map.matches(ResourceType::Map));
        assert!(ResourceType::Queue.matches(ResourceType::Queue));
    }

    #[test]
    fn test_resource_type_all_matches_everything() {
        assert!(ResourceType::All.matches(ResourceType::Map));
        assert!(ResourceType::All.matches(ResourceType::Queue));
        assert!(ResourceType::Map.matches(ResourceType::All));
        assert!(ResourceType::Queue.matches(ResourceType::All));
    }

    #[test]
    fn test_resource_type_does_not_match_others() {
        assert!(!ResourceType::Map.matches(ResourceType::Queue));
        assert!(!ResourceType::Queue.matches(ResourceType::Map));
    }

    #[test]
    fn test_permission_grant_wildcard_pattern() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_permission(Permission::Read);

        assert!(grant.allows(Permission::Read, ResourceType::Map, "any-map"));
        assert!(grant.allows(Permission::Read, ResourceType::Map, "another-map"));
    }

    #[test]
    fn test_permission_grant_exact_pattern() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_pattern("specific-map")
            .with_permission(Permission::Read);

        assert!(grant.allows(Permission::Read, ResourceType::Map, "specific-map"));
        assert!(!grant.allows(Permission::Read, ResourceType::Map, "other-map"));
    }

    #[test]
    fn test_permission_grant_prefix_pattern() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_pattern("orders-*")
            .with_permission(Permission::Read);

        assert!(grant.allows(Permission::Read, ResourceType::Map, "orders-2024"));
        assert!(grant.allows(Permission::Read, ResourceType::Map, "orders-archive"));
        assert!(!grant.allows(Permission::Read, ResourceType::Map, "customers"));
    }

    #[test]
    fn test_permission_grant_suffix_pattern() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_pattern("*-cache")
            .with_permission(Permission::Read);

        assert!(grant.allows(Permission::Read, ResourceType::Map, "user-cache"));
        assert!(grant.allows(Permission::Read, ResourceType::Map, "product-cache"));
        assert!(!grant.allows(Permission::Read, ResourceType::Map, "cache-backup"));
    }

    #[test]
    fn test_permission_grant_wrong_resource_type() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_permission(Permission::Read);

        assert!(!grant.allows(Permission::Read, ResourceType::Queue, "any-queue"));
    }

    #[test]
    fn test_permission_grant_wrong_permission() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_permission(Permission::Read);

        assert!(!grant.allows(Permission::Write, ResourceType::Map, "any-map"));
    }

    #[test]
    fn test_role_builder() {
        let role = Role::new("test-role")
            .with_grant(
                PermissionGrant::new(ResourceType::Map)
                    .with_permission(Permission::Read),
            );

        assert_eq!(role.name(), "test-role");
        assert_eq!(role.grants().len(), 1);
    }

    #[test]
    fn test_role_allows_permission() {
        let role = Role::new("reader")
            .with_grant(
                PermissionGrant::new(ResourceType::Map)
                    .with_permission(Permission::Read),
            );

        assert!(role.allows(Permission::Read, ResourceType::Map, "test-map"));
        assert!(!role.allows(Permission::Write, ResourceType::Map, "test-map"));
    }

    #[test]
    fn test_role_multiple_grants() {
        let role = Role::new("multi")
            .with_grants([
                PermissionGrant::new(ResourceType::Map)
                    .with_permission(Permission::Read),
                PermissionGrant::new(ResourceType::Queue)
                    .with_permission(Permission::Write),
            ]);

        assert!(role.allows(Permission::Read, ResourceType::Map, "m"));
        assert!(role.allows(Permission::Write, ResourceType::Queue, "q"));
        assert!(!role.allows(Permission::Write, ResourceType::Map, "m"));
    }

    #[test]
    fn test_role_read_only_preset() {
        let role = Role::read_only("reader");

        assert!(role.allows(Permission::Read, ResourceType::Map, "any"));
        assert!(role.allows(Permission::Listen, ResourceType::Queue, "any"));
        assert!(!role.allows(Permission::Write, ResourceType::Map, "any"));
        assert!(!role.allows(Permission::Create, ResourceType::Map, "any"));
    }

    #[test]
    fn test_role_read_write_preset() {
        let role = Role::read_write("editor");

        assert!(role.allows(Permission::Read, ResourceType::Map, "any"));
        assert!(role.allows(Permission::Write, ResourceType::Map, "any"));
        assert!(role.allows(Permission::Put, ResourceType::Queue, "any"));
        assert!(role.allows(Permission::Remove, ResourceType::Map, "any"));
        assert!(!role.allows(Permission::Create, ResourceType::Map, "any"));
        assert!(!role.allows(Permission::Destroy, ResourceType::Map, "any"));
    }

    #[test]
    fn test_role_admin_preset() {
        let role = Role::admin("admin");

        assert!(role.allows(Permission::Read, ResourceType::Map, "any"));
        assert!(role.allows(Permission::Write, ResourceType::Queue, "any"));
        assert!(role.allows(Permission::Create, ResourceType::List, "any"));
        assert!(role.allows(Permission::Destroy, ResourceType::Set, "any"));
        assert!(role.allows(Permission::Lock, ResourceType::Map, "any"));
    }

    #[test]
    fn test_authorization_context_check_permission_granted() {
        let ctx = AuthorizationContext::new()
            .with_role(Role::read_only("reader"));

        assert!(ctx.check_permission(Permission::Read, ResourceType::Map, "test").is_ok());
    }

    #[test]
    fn test_authorization_context_check_permission_denied() {
        let ctx = AuthorizationContext::new()
            .with_role(Role::read_only("reader"));

        let result = ctx.check_permission(Permission::Write, ResourceType::Map, "test");
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.permission, Permission::Write);
        assert_eq!(err.resource_type, ResourceType::Map);
        assert_eq!(err.resource_name, "test");
    }

    #[test]
    fn test_authorization_context_no_roles() {
        let ctx = AuthorizationContext::new();

        let result = ctx.check_permission(Permission::Read, ResourceType::Map, "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("no roles assigned"));
    }

    #[test]
    fn test_authorization_context_disabled() {
        let ctx = AuthorizationContext::disabled();

        assert!(ctx.check_permission(Permission::Write, ResourceType::Map, "test").is_ok());
        assert!(ctx.check_permission(Permission::Destroy, ResourceType::Queue, "q").is_ok());
    }

    #[test]
    fn test_authorization_context_multiple_roles() {
        let ctx = AuthorizationContext::new()
            .with_roles([
                Role::new("map-reader").with_grant(
                    PermissionGrant::new(ResourceType::Map)
                        .with_permission(Permission::Read),
                ),
                Role::new("queue-writer").with_grant(
                    PermissionGrant::new(ResourceType::Queue)
                        .with_permission(Permission::Write),
                ),
            ]);

        assert!(ctx.check_permission(Permission::Read, ResourceType::Map, "m").is_ok());
        assert!(ctx.check_permission(Permission::Write, ResourceType::Queue, "q").is_ok());
        assert!(ctx.check_permission(Permission::Write, ResourceType::Map, "m").is_err());
    }

    #[test]
    fn test_check_map_operations() {
        let ctx = AuthorizationContext::new()
            .with_role(Role::read_write("editor"));

        assert!(ctx.check_map_read("users").is_ok());
        assert!(ctx.check_map_write("users").is_ok());
        assert!(ctx.check_map_put("users").is_ok());
        assert!(ctx.check_map_remove("users").is_ok());
        assert!(ctx.check_map_create("users").is_err());
        assert!(ctx.check_map_destroy("users").is_err());
    }

    #[test]
    fn test_check_queue_operations() {
        let ctx = AuthorizationContext::new()
            .with_role(Role::read_write("editor"));

        assert!(ctx.check_queue_read("tasks").is_ok());
        assert!(ctx.check_queue_write("tasks").is_ok());
        assert!(ctx.check_queue_put("tasks").is_ok());
        assert!(ctx.check_queue_remove("tasks").is_ok());
    }

    #[test]
    fn test_permission_denied_display() {
        let err = PermissionDenied {
            permission: Permission::Write,
            resource_type: ResourceType::Map,
            resource_name: "users".to_string(),
            message: "access denied".to_string(),
        };

        let display = err.to_string();
        assert!(display.contains("Write"));
        assert!(display.contains("Map"));
        assert!(display.contains("users"));
        assert!(display.contains("access denied"));
    }

    #[test]
    fn test_permission_denied_is_error() {
        fn assert_error<T: std::error::Error>() {}
        assert_error::<PermissionDenied>();
    }

    #[test]
    fn test_authorization_context_set_enabled() {
        let mut ctx = AuthorizationContext::new();
        assert!(ctx.is_enabled());

        ctx.set_enabled(false);
        assert!(!ctx.is_enabled());

        assert!(ctx.check_permission(Permission::Destroy, ResourceType::Map, "x").is_ok());
    }

    #[test]
    fn test_permission_grant_all_permissions() {
        let grant = PermissionGrant::new(ResourceType::Map)
            .with_all_permissions();

        assert!(grant.allows(Permission::Read, ResourceType::Map, "m"));
        assert!(grant.allows(Permission::Write, ResourceType::Map, "m"));
        assert!(grant.allows(Permission::Create, ResourceType::Map, "m"));
        assert!(grant.allows(Permission::Destroy, ResourceType::Map, "m"));
    }

    #[test]
    fn test_permission_grant_multiple_permissions() {
        let grant = PermissionGrant::new(ResourceType::Queue)
            .with_permissions([Permission::Read, Permission::Put, Permission::Remove]);

        assert!(grant.allows(Permission::Read, ResourceType::Queue, "q"));
        assert!(grant.allows(Permission::Put, ResourceType::Queue, "q"));
        assert!(grant.allows(Permission::Remove, ResourceType::Queue, "q"));
        assert!(!grant.allows(Permission::Create, ResourceType::Queue, "q"));
    }

    #[test]
    fn test_permission_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<Permission>();
    }

    #[test]
    fn test_resource_type_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ResourceType>();
    }

    #[test]
    fn test_role_clone() {
        let role = Role::admin("admin");
        let cloned = role.clone();

        assert_eq!(cloned.name(), role.name());
        assert_eq!(cloned.grants().len(), role.grants().len());
    }

    #[test]
    fn test_authorization_context_clone() {
        let ctx = AuthorizationContext::new()
            .with_role(Role::admin("admin"));

        let cloned = ctx.clone();
        assert_eq!(cloned.roles().len(), ctx.roles().len());
        assert_eq!(cloned.is_enabled(), ctx.is_enabled());
    }

    #[test]
    fn test_map_read_only_role_denies_write() {
        let role = Role::new("map-reader")
            .with_grant(
                PermissionGrant::new(ResourceType::Map)
                    .with_pattern("public-*")
                    .with_permission(Permission::Read),
            );

        let ctx = AuthorizationContext::new().with_role(role);

        assert!(ctx.check_map_read("public-data").is_ok());
        assert!(ctx.check_map_read("public-config").is_ok());
        assert!(ctx.check_map_write("public-data").is_err());
        assert!(ctx.check_map_read("private-data").is_err());
    }

    #[test]
    fn test_queue_producer_consumer_roles() {
        let producer = Role::new("producer")
            .with_grant(
                PermissionGrant::new(ResourceType::Queue)
                    .with_permission(Permission::Put),
            );

        let consumer = Role::new("consumer")
            .with_grant(
                PermissionGrant::new(ResourceType::Queue)
                    .with_permission(Permission::Read)
                    .with_permission(Permission::Remove),
            );

        let producer_ctx = AuthorizationContext::new().with_role(producer);
        assert!(producer_ctx.check_queue_put("tasks").is_ok());
        assert!(producer_ctx.check_queue_remove("tasks").is_err());

        let consumer_ctx = AuthorizationContext::new().with_role(consumer);
        assert!(consumer_ctx.check_queue_read("tasks").is_ok());
        assert!(consumer_ctx.check_queue_remove("tasks").is_ok());
        assert!(consumer_ctx.check_queue_put("tasks").is_err());
    }
}
