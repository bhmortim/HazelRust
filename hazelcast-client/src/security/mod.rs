//! Security module for authentication and authorization.

pub mod authenticator;

pub use authenticator::{
    AuthError, AuthResponse, Authenticator, Credentials, CustomCredentials, DefaultAuthenticator,
    JwtValidationResult, TokenAuthenticator, TokenCredentials, TokenFormat, validate_jwt_structure,
};
