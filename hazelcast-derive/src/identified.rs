//! Derive macro implementation for `IdentifiedDataSerializable`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit};

pub fn derive_identified_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Parse required struct-level attributes
    let factory_id = parse_int_attr(&input.attrs, "factory_id")
        .expect("IdentifiedDataSerializable requires #[hazelcast(factory_id = N)]");
    let class_id = parse_int_attr(&input.attrs, "class_id")
        .expect("IdentifiedDataSerializable requires #[hazelcast(class_id = N)]");

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("IdentifiedDataSerializable only supports structs with named fields"),
        },
        _ => panic!("IdentifiedDataSerializable can only be derived for structs"),
    };

    let mut write_stmts = Vec::new();
    let mut read_stmts = Vec::new();

    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;

        if has_skip_attr(&field.attrs) {
            read_stmts.push(quote! { self.#field_ident = Default::default(); });
            continue;
        }

        let ty_str = type_to_string(field_ty);
        let write = match ty_str.as_str() {
            "bool" => quote! { output.write_bool(self.#field_ident)?; },
            "i8" => quote! { output.write_byte(self.#field_ident)?; },
            "i16" => quote! { output.write_short(self.#field_ident)?; },
            "i32" => quote! { output.write_int(self.#field_ident)?; },
            "i64" => quote! { output.write_long(self.#field_ident)?; },
            "f32" => quote! { output.write_float(self.#field_ident)?; },
            "f64" => quote! { output.write_double(self.#field_ident)?; },
            "String" => quote! { output.write_string(&self.#field_ident)?; },
            _ => quote! { output.write_string(&format!("{:?}", self.#field_ident))?; },
        };
        write_stmts.push(write);

        let read = match ty_str.as_str() {
            "bool" => quote! { self.#field_ident = input.read_bool()?; },
            "i8" => quote! { self.#field_ident = input.read_byte()?; },
            "i16" => quote! { self.#field_ident = input.read_short()?; },
            "i32" => quote! { self.#field_ident = input.read_int()?; },
            "i64" => quote! { self.#field_ident = input.read_long()?; },
            "f32" => quote! { self.#field_ident = input.read_float()?; },
            "f64" => quote! { self.#field_ident = input.read_double()?; },
            "String" => quote! { self.#field_ident = input.read_string()?; },
            _ => quote! { self.#field_ident = Default::default(); },
        };
        read_stmts.push(read);
    }

    let factory_id_i32 = factory_id as i32;
    let class_id_i32 = class_id as i32;

    let expanded = quote! {
        impl #impl_generics hazelcast_core::serialization::IdentifiedDataSerializable
            for #name #ty_generics #where_clause
        {
            fn factory_id(&self) -> i32 {
                #factory_id_i32
            }

            fn class_id(&self) -> i32 {
                #class_id_i32
            }

            fn write_data(
                &self,
                output: &mut dyn hazelcast_core::DataOutput,
            ) -> hazelcast_core::Result<()> {
                #(#write_stmts)*
                Ok(())
            }

            fn read_data(
                &mut self,
                input: &mut dyn hazelcast_core::DataInput,
            ) -> hazelcast_core::Result<()> {
                #(#read_stmts)*
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
