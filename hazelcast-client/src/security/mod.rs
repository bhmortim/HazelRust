//! Security module for authentication and authorization.

pub mod authenticator;
pub mod authorization;

pub use authenticator::{
    AuthError, AuthResponse, Authenticator, Credentials, CustomCredentials, DefaultAuthenticator,
    JwtValidationResult, TokenAuthenticator, TokenCredentials, TokenFormat, validate_jwt_structure,
};

pub use authorization::{
    AuthorizationContext, Permission, PermissionDenied, PermissionGrant, ResourceType, Role,
};
