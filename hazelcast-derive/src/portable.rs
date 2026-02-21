//! Derive macro implementation for `HazelcastPortable`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit};

pub fn derive_portable_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Parse required struct-level attributes
    let factory_id = parse_int_attr(&input.attrs, "factory_id")
        .expect("HazelcastPortable requires #[hazelcast(factory_id = N)]");
    let class_id = parse_int_attr(&input.attrs, "class_id")
        .expect("HazelcastPortable requires #[hazelcast(class_id = N)]");
    let version = parse_int_attr(&input.attrs, "version").unwrap_or(0);

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("HazelcastPortable only supports structs with named fields"),
        },
        _ => panic!("HazelcastPortable can only be derived for structs"),
    };

    let mut write_stmts = Vec::new();
    let mut read_stmts = Vec::new();

    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;

        if has_skip_attr(&field.attrs) {
            read_stmts.push(quote! { #field_ident: Default::default() });
            continue;
        }

        let wire_name = parse_str_attr(&field.attrs, "field_name")
            .unwrap_or_else(|| field_ident.to_string());

        let ty_str = type_to_string(field_ty);
        let write = match ty_str.as_str() {
            "bool" => quote! { writer.write_boolean(#wire_name, self.#field_ident)?; },
            "i8" => quote! { writer.write_byte(#wire_name, self.#field_ident)?; },
            "i16" => quote! { writer.write_short(#wire_name, self.#field_ident)?; },
            "i32" => quote! { writer.write_int(#wire_name, self.#field_ident)?; },
            "i64" => quote! { writer.write_long(#wire_name, self.#field_ident)?; },
            "f32" => quote! { writer.write_float(#wire_name, self.#field_ident)?; },
            "f64" => quote! { writer.write_double(#wire_name, self.#field_ident)?; },
            "String" => quote! { writer.write_string(#wire_name, &self.#field_ident)?; },
            _ => quote! { writer.write_string(#wire_name, &format!("{:?}", self.#field_ident))?; },
        };
        write_stmts.push(write);

        let read = match ty_str.as_str() {
            "bool" => quote! { #field_ident: reader.read_boolean(#wire_name)? },
            "i8" => quote! { #field_ident: reader.read_byte(#wire_name)? },
            "i16" => quote! { #field_ident: reader.read_short(#wire_name)? },
            "i32" => quote! { #field_ident: reader.read_int(#wire_name)? },
            "i64" => quote! { #field_ident: reader.read_long(#wire_name)? },
            "f32" => quote! { #field_ident: reader.read_float(#wire_name)? },
            "f64" => quote! { #field_ident: reader.read_double(#wire_name)? },
            "String" => quote! { #field_ident: reader.read_string(#wire_name)? },
            _ => quote! { #field_ident: Default::default() },
        };
        read_stmts.push(read);
    }

    let factory_id_i32 = factory_id as i32;
    let class_id_i32 = class_id as i32;
    let version_i32 = version as i32;

    let expanded = quote! {
        impl #impl_generics hazelcast_core::Portable for #name #ty_generics #where_clause {
            fn factory_id(&self) -> i32 {
                #factory_id_i32
            }

            fn class_id(&self) -> i32 {
                #class_id_i32
            }

            fn version(&self) -> i32 {
                #version_i32
            }

            fn write_portable(&self, writer: &mut dyn hazelcast_core::PortableWriter) -> hazelcast_core::Result<()> {
                #(#write_stmts)*
                Ok(())
            }

            fn read_portable(&mut self, reader: &mut dyn hazelcast_core::PortableReader) -> hazelcast_core::Result<()> {
                let _tmp = Self {
                    #(#read_stmts,)*
                };
                *self = _tmp;
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}

fn type_to_string(ty: &syn::Type) -> String {
    quote!(#ty).to_string().replace(' ', "")
}

fn parse_int_attr(attrs: &[syn::Attribute], key: &str) -> Option<i64> {
    for attr in attrs {
        if !attr.path().is_ident("hazelcast") {
            continue;
        }
        let mut result = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident(key) {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Int(int_lit) = lit {
                    result = int_lit.base10_parse().ok();
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

fn parse_str_attr(attrs: &[syn::Attribute], key: &str) -> Option<String> {
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
