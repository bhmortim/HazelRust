//! Derive macro implementation for `HazelcastCompact`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit};

pub fn derive_compact_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Parse struct-level attributes
    let type_name = parse_struct_attr(&input.attrs, "type_name")
        .unwrap_or_else(|| name.to_string());

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("HazelcastCompact only supports structs with named fields"),
        },
        _ => panic!("HazelcastCompact can only be derived for structs"),
    };

    let mut write_stmts = Vec::new();
    let mut read_stmts = Vec::new();
    let mut field_names = Vec::new();

    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;

        // Check for skip attribute
        if has_skip_attr(&field.attrs) {
            field_names.push(quote! { #field_ident: Default::default() });
            continue;
        }

        // Get wire field name (default to Rust field name)
        let wire_name = parse_struct_attr(&field.attrs, "field_name")
            .unwrap_or_else(|| field_ident.to_string());

        let write_stmt = generate_write_stmt(field_ident, field_ty, &wire_name);
        let read_stmt = generate_read_stmt(field_ident, field_ty, &wire_name);

        write_stmts.push(write_stmt);
        read_stmts.push(read_stmt.clone());
        field_names.push(quote! { #field_ident: #read_stmt });
    }

    let expanded = quote! {
        impl #impl_generics hazelcast_core::Compact for #name #ty_generics #where_clause {
            fn get_type_name() -> &'static str {
                #type_name
            }

            fn write(&self, writer: &mut dyn hazelcast_core::CompactWriter) -> hazelcast_core::Result<()> {
                #(#write_stmts)*
                Ok(())
            }

            fn read(reader: &mut dyn hazelcast_core::CompactReader) -> hazelcast_core::Result<Self> {
                Ok(Self {
                    #(#field_names,)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

fn generate_write_stmt(
    field_ident: &syn::Ident,
    field_ty: &syn::Type,
    wire_name: &str,
) -> proc_macro2::TokenStream {
    let ty_str = type_to_string(field_ty);
    match ty_str.as_str() {
        "bool" => quote! { writer.write_boolean(#wire_name, self.#field_ident)?; },
        "i8" => quote! { writer.write_int8(#wire_name, self.#field_ident)?; },
        "i16" => quote! { writer.write_int16(#wire_name, self.#field_ident)?; },
        "i32" => quote! { writer.write_int32(#wire_name, self.#field_ident)?; },
        "i64" => quote! { writer.write_int64(#wire_name, self.#field_ident)?; },
        "f32" => quote! { writer.write_float32(#wire_name, self.#field_ident)?; },
        "f64" => quote! { writer.write_float64(#wire_name, self.#field_ident)?; },
        "String" => quote! { writer.write_string(#wire_name, Some(&self.#field_ident))?; },
        s if s.starts_with("Option<") => {
            let inner = extract_inner_type(s);
            match inner.as_str() {
                "bool" => quote! { writer.write_nullable_boolean(#wire_name, self.#field_ident)?; },
                "i8" => quote! { writer.write_nullable_int8(#wire_name, self.#field_ident)?; },
                "i16" => quote! { writer.write_nullable_int16(#wire_name, self.#field_ident)?; },
                "i32" => quote! { writer.write_nullable_int32(#wire_name, self.#field_ident)?; },
                "i64" => quote! { writer.write_nullable_int64(#wire_name, self.#field_ident)?; },
                "f32" => quote! { writer.write_nullable_float32(#wire_name, self.#field_ident)?; },
                "f64" => quote! { writer.write_nullable_float64(#wire_name, self.#field_ident)?; },
                "String" => quote! { writer.write_string(#wire_name, self.#field_ident.as_deref())?; },
                _ => quote! { writer.write_string(#wire_name, self.#field_ident.as_ref().map(|v| v.to_string()).as_deref())?; },
            }
        }
        s if s.starts_with("Vec<") => {
            let inner = extract_inner_type(s);
            match inner.as_str() {
                "bool" => quote! { writer.write_array_of_boolean(#wire_name, Some(&self.#field_ident))?; },
                "i8" => quote! { writer.write_array_of_int8(#wire_name, Some(&self.#field_ident))?; },
                "i16" => quote! { writer.write_array_of_int16(#wire_name, Some(&self.#field_ident))?; },
                "i32" => quote! { writer.write_array_of_int32(#wire_name, Some(&self.#field_ident))?; },
                "i64" => quote! { writer.write_array_of_int64(#wire_name, Some(&self.#field_ident))?; },
                "f32" => quote! { writer.write_array_of_float32(#wire_name, Some(&self.#field_ident))?; },
                "f64" => quote! { writer.write_array_of_float64(#wire_name, Some(&self.#field_ident))?; },
                _ => quote! { writer.write_string(#wire_name, Some(&format!("{:?}", self.#field_ident)))?; },
            }
        }
        _ => quote! { writer.write_string(#wire_name, Some(&format!("{:?}", self.#field_ident)))?; },
    }
}

fn generate_read_stmt(
    _field_ident: &syn::Ident,
    field_ty: &syn::Type,
    wire_name: &str,
) -> proc_macro2::TokenStream {
    let ty_str = type_to_string(field_ty);
    match ty_str.as_str() {
        "bool" => quote! { reader.read_boolean(#wire_name)? },
        "i8" => quote! { reader.read_int8(#wire_name)? },
        "i16" => quote! { reader.read_int16(#wire_name)? },
        "i32" => quote! { reader.read_int32(#wire_name)? },
        "i64" => quote! { reader.read_int64(#wire_name)? },
        "f32" => quote! { reader.read_float32(#wire_name)? },
        "f64" => quote! { reader.read_float64(#wire_name)? },
        "String" => quote! { reader.read_string(#wire_name)?.unwrap_or_default() },
        s if s.starts_with("Option<") => {
            let inner = extract_inner_type(s);
            match inner.as_str() {
                "bool" => quote! { reader.read_nullable_boolean(#wire_name)? },
                "i8" => quote! { reader.read_nullable_int8(#wire_name)? },
                "i16" => quote! { reader.read_nullable_int16(#wire_name)? },
                "i32" => quote! { reader.read_nullable_int32(#wire_name)? },
                "i64" => quote! { reader.read_nullable_int64(#wire_name)? },
                "f32" => quote! { reader.read_nullable_float32(#wire_name)? },
                "f64" => quote! { reader.read_nullable_float64(#wire_name)? },
                "String" => quote! { reader.read_string(#wire_name)? },
                _ => quote! { None },
            }
        }
        s if s.starts_with("Vec<") => {
            let inner = extract_inner_type(s);
            match inner.as_str() {
                "bool" => quote! { reader.read_array_of_boolean(#wire_name)?.unwrap_or_default() },
                "i8" => quote! { reader.read_array_of_int8(#wire_name)?.unwrap_or_default() },
                "i16" => quote! { reader.read_array_of_int16(#wire_name)?.unwrap_or_default() },
                "i32" => quote! { reader.read_array_of_int32(#wire_name)?.unwrap_or_default() },
                "i64" => quote! { reader.read_array_of_int64(#wire_name)?.unwrap_or_default() },
                "f32" => quote! { reader.read_array_of_float32(#wire_name)?.unwrap_or_default() },
                "f64" => quote! { reader.read_array_of_float64(#wire_name)?.unwrap_or_default() },
                _ => quote! { Vec::new() },
            }
        }
        _ => quote! { Default::default() },
    }
}

fn type_to_string(ty: &syn::Type) -> String {
    quote!(#ty).to_string().replace(' ', "")
}

fn extract_inner_type(s: &str) -> String {
    // Extract T from Option<T> or Vec<T>
    let start = s.find('<').unwrap() + 1;
    let end = s.rfind('>').unwrap();
    s[start..end].to_string()
}

fn parse_struct_attr(attrs: &[syn::Attribute], key: &str) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("hazelcast") {
            continue;
        }
        let mut result = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident(key) {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result = Some(s.value());
                }
            }
            Ok(())
        });
        if result.is_some() {
            return result;
        }
    }
    None
}

fn has_skip_attr(attrs: &[syn::Attribute]) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("hazelcast") {
            continue;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                found = true;
            }
            Ok(())
        });
        if found {
            return true;
        }
    }
    false
}
