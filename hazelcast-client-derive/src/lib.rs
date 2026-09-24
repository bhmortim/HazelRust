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

//! Derive macros for Hazelcast serialization formats.
//!
//! This crate provides three derive macros:
//!
//! - [`HazelcastCompact`] — generates a [`Compact`] implementation for the newer
//!   schema-less serialization format.
//! - [`HazelcastPortable`] — generates a [`Portable`] implementation for the
//!   schema-based serialization format compatible with Java/Python/.NET.
//! - [`IdentifiedDataSerializable`] — generates an [`IdentifiedDataSerializable`]
//!   implementation for the fast factory-based serialization format.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client_derive::HazelcastCompact;
//!
//! #[derive(HazelcastCompact)]
//! #[hazelcast(type_name = "com.example.Person")]
//! struct Person {
//!     name: String,
//!     age: i32,
//!     #[hazelcast(field_name = "emailAddress")]
//!     email: Option<String>,
//! }
//! ```

extern crate proc_macro;

mod compact;
mod identified;
mod portable;

use proc_macro::TokenStream;

/// Derives the `Compact` trait for a struct.
///
/// # Attributes
///
/// ## Struct-level
/// - `#[hazelcast(type_name = "...")]` — sets the compact type name (defaults to
///   the Rust struct name).
///
/// ## Field-level
/// - `#[hazelcast(field_name = "...")]` — overrides the wire field name (defaults
///   to the Rust field name).
/// - `#[hazelcast(skip)]` — skips this field during serialization.
///
/// # Supported Field Types
///
/// `bool`, `i8`, `i16`, `i32`, `i64`, `f32`, `f64`, `String`,
/// `Option<T>` (nullable variants), `Vec<T>` (array variants).
#[proc_macro_derive(HazelcastCompact, attributes(hazelcast))]
pub fn derive_compact(input: TokenStream) -> TokenStream {
    compact::derive_compact_impl(input)
}

/// Derives the `Portable` trait for a struct.
///
/// # Attributes
///
/// ## Struct-level
/// - `#[hazelcast(factory_id = N)]` — **required**. The portable factory ID.
/// - `#[hazelcast(class_id = N)]` — **required**. The portable class ID.
/// - `#[hazelcast(version = N)]` — the portable class version (defaults to 0).
///
/// ## Field-level
/// - `#[hazelcast(field_name = "...")]` — overrides the wire field name.
/// - `#[hazelcast(skip)]` — skips this field during serialization.
#[proc_macro_derive(HazelcastPortable, attributes(hazelcast))]
pub fn derive_portable(input: TokenStream) -> TokenStream {
    portable::derive_portable_impl(input)
}

/// Derives the `IdentifiedDataSerializable` trait for a struct.
///
/// # Attributes
///
/// ## Struct-level
/// - `#[hazelcast(factory_id = N)]` — **required**. The factory ID.
/// - `#[hazelcast(class_id = N)]` — **required**. The class ID.
///
/// ## Field-level
/// - `#[hazelcast(skip)]` — skips this field during serialization.
#[proc_macro_derive(IdentifiedDataSerializable, attributes(hazelcast))]
pub fn derive_identified(input: TokenStream) -> TokenStream {
    identified::derive_identified_impl(input)
}
