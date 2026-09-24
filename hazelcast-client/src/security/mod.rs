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

//! Security module for authentication and authorization.

pub mod authenticator;
pub mod authorization;
pub mod credentials;
pub mod tls_config;

#[cfg(feature = "kerberos")]
pub mod kerberos;

pub use authenticator::{
    validate_jwt_structure, AuthError, AuthResponse, Authenticator, Credentials, CustomCredentials,
    DefaultAuthenticator, JwtValidationResult, TokenAuthenticator, TokenCredentials, TokenFormat,
};

pub use authorization::{
    AuthorizationContext, Permission, PermissionDenied, PermissionGrant, ResourceType, Role,
};

pub use credentials::{CredentialError, CredentialProvider, EnvironmentCredentialProvider};

pub use tls_config::{
    cipher_suites, HostnameVerification, TlsConfig, TlsConfigBuilder, TlsConfigError,
    TlsProtocolVersion,
};

#[cfg(feature = "aws")]
pub use credentials::AwsCredentialProvider;

#[cfg(feature = "azure")]
pub use credentials::AzureCredentialProvider;

#[cfg(feature = "gcp")]
pub use credentials::GcpCredentialProvider;

#[cfg(feature = "kubernetes")]
pub use credentials::KubernetesCredentialProvider;

#[cfg(feature = "kerberos")]
pub use kerberos::{KerberosAuthenticator, KerberosCredentials};
