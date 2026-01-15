Based on the `HazelRust` codebase, here is the concise coding style guide focusing on its specific architectural patterns and modern Rust idioms.

# HazelRust Coding Style Guide

## 1. Safety and Interior Mutability
*   **Avoid `unsafe` where possible:** Rely on the compiler to manage memory and lifetimes.
*   **Prefer `RefCell` for local interior mutability:** Use `RefCell` when you need to mutate data behind an immutable reference within a single-threaded context. 
*   **Pattern Matching for State Management:** Use `match` extensively on `State` enums to handle internal transitions safely.

## 2. Structural Patterns and Lifecycle
*   **The "Application" Trait Pattern:** Define core logic via a trait (e.g., `Application`) that provides hooks for `on_update`, `on_event`, and `on_render`.
*   **Static Creation Methods:** Use a `new()` associated function as the primary constructor. Avoid complex logic in the constructor; delegate to initialization methods if needed.
*   **Explicit Drop Logic:** Implement the `Drop` trait for structs managing external resources (like window handles or GPU contexts) to ensure clean teardown.

## 3. Macros and Logging
*   **Contextual Logging:** Use the `info!`, `warn!`, and `error!` macros consistently. 
*   **Module-Level Metadata:** Use the `#[macro_use]` attribute on the internal `macros` module to ensure logging and utility macros are available throughout the crate without explicit imports.

## 4. Error Handling and Assertions
*   **Internal Assertions:** Use `hz_core_assert!` for engine-level invariants and `hz_assert!` for application-level logic. These should be used to catch unrecoverable state violations during development.
*   **Result Wrapping:** Prefer returning `Result<(), String>` or specialized Error types for fallible initialization (e.g., Window creation).

## 5. Event Handling
*   **Dispatcher Pattern:** Use a `Dispatcher` mechanism to route events to specific handlers. 
*   **Functional Event Processing:** Leverage closures and the `?` operator within event dispatchers to create a clean, declarative event flow:
    ```rust
    dispatcher.dispatch::<WindowCloseEvent>(&|e| self.on_window_close(e))?;
    ```

## 6. Naming and Visibility
*   **Internal vs. Public:** Use `pub(crate)` for modules and functions that should be accessible across the engine but hidden from the end-user.
*   **Trait Prefixes:** Do not prefix traits with `I` (e.g., use `Layer`, not `ILayer`).
*   **Feature Gating:** Use `#[cfg(target_os = "...")]` at the module level to handle platform-specific implementations (e.g., Windows vs. Linux windowing) rather than cluttering logic with inline checks.

## 7. Formatting and Idioms
*   **Trailing Commas:** Always use trailing commas in multi-line struct definitions and match arms to minimize diff noise.
*   **Explicit `self` types:** Use `self: &mut Self` or `self: RefCell<Self>` only when specific ownership semantics are required; otherwise, stick to standard `&self` and `&mut self`.