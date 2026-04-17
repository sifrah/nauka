//! Proc-macros for the Nauka Resource contract — see ADR 0006.
//!
//! Exposes one attribute macro: `#[resource(table = "…", scope = "…")]`.
//! Apply it to a struct and the macro will:
//!
//! 1. Validate the table name (snake_case) and scope (`local` /
//!    `cluster`).
//! 2. Require exactly one `#[id]` field, reject `#[unique]` on
//!    non-scalar types, and reject hand-written `created_at` /
//!    `updated_at` / `version` fields (those are injected).
//! 3. Inject `created_at: Datetime`, `updated_at: Datetime`,
//!    `version: u64` into the struct.
//! 4. Generate the full `SCHEMAFULL` DDL (`DEFINE TABLE` / `FIELD` /
//!    `INDEX`) as a `&'static str`.
//! 5. Implement `nauka_core::resource::Resource`.
//! 6. Register the resource into `nauka_core::resource::ALL_RESOURCES`
//!    via a `linkme::distributed_slice` entry.
//!
//! Anything that violates the contract is a **compile error**. See
//! `tests/compile_fail/` in this crate for the exhaustive list.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    parse_macro_input, parse_quote, Field, Fields, GenericArgument, Ident, ItemStruct, LitStr,
    PathArguments, PathSegment, Token, Type,
};

#[proc_macro_attribute]
pub fn resource(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ResourceArgs);
    let item = parse_macro_input!(input as ItemStruct);

    match expand(args, item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// -------- argument parsing --------

struct ResourceArgs {
    table: LitStr,
    scope: LitStr,
}

impl Parse for ResourceArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut table: Option<LitStr> = None;
        let mut scope: Option<LitStr> = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;
            match key.to_string().as_str() {
                "table" => {
                    if table.is_some() {
                        return Err(syn::Error::new(key.span(), "`table` specified more than once"));
                    }
                    table = Some(value);
                }
                "scope" => {
                    if scope.is_some() {
                        return Err(syn::Error::new(key.span(), "`scope` specified more than once"));
                    }
                    scope = Some(value);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown attribute key `{other}` (allowed: `table`, `scope`)"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        let table = table.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[resource]` requires `table = \"…\"`",
            )
        })?;
        let scope = scope.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[resource]` requires `scope = \"local\"` or `scope = \"cluster\"`",
            )
        })?;

        Ok(ResourceArgs { table, scope })
    }
}

#[derive(Clone, Copy)]
enum ParsedScope {
    Local,
    Cluster,
}

// -------- expansion --------

fn expand(args: ResourceArgs, mut item: ItemStruct) -> syn::Result<TokenStream2> {
    let table = args.table.value();
    validate_table_name(&args.table)?;
    let scope = parse_scope(&args.scope)?;

    // Snapshot what we need before mutating `item`.
    let (id_field_name, id_field_type) = {
        let id_field = find_id_field(&item)?;
        (
            id_field.ident.clone().expect("named field has ident"),
            id_field.ty.clone(),
        )
    };

    validate_unique_fields(&item)?;
    validate_no_base_field_collision(&item)?;

    let user_ddl = generate_user_field_ddl(&table, &item)?;
    let unique_ddl = generate_unique_indexes(&table, &item);
    let base_ddl = generate_base_field_ddl(&table);
    let full_ddl = format!(
        "DEFINE TABLE IF NOT EXISTS {table} SCHEMAFULL;\n{user_ddl}{base_ddl}{unique_ddl}"
    );

    inject_base_fields(&mut item);
    strip_macro_attrs(&mut item);

    let struct_name = item.ident.clone();
    let scope_path = match scope {
        ParsedScope::Local => quote!(::nauka_core::resource::Scope::Local),
        ParsedScope::Cluster => quote!(::nauka_core::resource::Scope::Cluster),
    };

    let static_name = format_ident!("__NAUKA_RES_{}", struct_name.to_string().to_uppercase());

    Ok(quote! {
        #item

        impl ::nauka_core::resource::Resource for #struct_name {
            const TABLE: &'static str = #table;
            const SCOPE: ::nauka_core::resource::Scope = #scope_path;
            const DDL: &'static str = #full_ddl;

            type Id = #id_field_type;

            fn id(&self) -> &Self::Id {
                &self.#id_field_name
            }

            fn created_at(&self) -> &::nauka_core::resource::Datetime {
                &self.created_at
            }

            fn updated_at(&self) -> &::nauka_core::resource::Datetime {
                &self.updated_at
            }

            fn version(&self) -> u64 {
                self.version
            }
        }

        #[::nauka_core::resource::__macro_support::linkme::distributed_slice(
            ::nauka_core::resource::ALL_RESOURCES
        )]
        #[linkme(crate = ::nauka_core::resource::__macro_support::linkme)]
        #[allow(non_upper_case_globals)]
        static #static_name: &::nauka_core::resource::ResourceDescriptor =
            &::nauka_core::resource::ResourceDescriptor {
                table: #table,
                scope: #scope_path,
                ddl: #full_ddl,
            };
    })
}

// -------- validators --------

fn validate_table_name(lit: &LitStr) -> syn::Result<()> {
    let name = lit.value();
    if name.is_empty() {
        return Err(syn::Error::new(lit.span(), "table name cannot be empty"));
    }

    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return Err(syn::Error::new(
            lit.span(),
            format!("table name `{name}` must start with a lowercase ASCII letter (snake_case)"),
        ));
    }

    for &b in bytes {
        let ok = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
        if !ok {
            return Err(syn::Error::new(
                lit.span(),
                format!(
                    "table name `{name}` must be snake_case \
                     (lowercase ASCII letters, digits, and `_` only)"
                ),
            ));
        }
    }

    if name.starts_with('_') || name.ends_with('_') || name.contains("__") {
        return Err(syn::Error::new(
            lit.span(),
            format!(
                "table name `{name}` must be snake_case \
                 (no leading, trailing, or doubled underscores)"
            ),
        ));
    }

    Ok(())
}

fn parse_scope(lit: &LitStr) -> syn::Result<ParsedScope> {
    match lit.value().as_str() {
        "local" => Ok(ParsedScope::Local),
        "cluster" => Ok(ParsedScope::Cluster),
        other => Err(syn::Error::new(
            lit.span(),
            format!("scope `{other}` is invalid — must be `\"local\"` or `\"cluster\"`"),
        )),
    }
}

fn find_id_field(item: &ItemStruct) -> syn::Result<&Field> {
    let Fields::Named(named) = &item.fields else {
        return Err(syn::Error::new_spanned(
            &item.fields,
            "`#[resource]` requires a struct with named fields",
        ));
    };

    let id_fields: Vec<&Field> = named
        .named
        .iter()
        .filter(|f| f.attrs.iter().any(|a| a.path().is_ident("id")))
        .collect();

    match id_fields.as_slice() {
        [] => Err(syn::Error::new_spanned(
            item,
            "`#[resource]` requires exactly one field marked `#[id]` — the natural key",
        )),
        [one] => Ok(*one),
        [_, second, ..] => Err(syn::Error::new_spanned(
            second,
            "`#[id]` may appear on only one field per resource",
        )),
    }
}

fn validate_unique_fields(item: &ItemStruct) -> syn::Result<()> {
    let Fields::Named(named) = &item.fields else {
        return Ok(());
    };

    for field in &named.named {
        if !field.attrs.iter().any(|a| a.path().is_ident("unique")) {
            continue;
        }
        if let Type::Path(tp) = &field.ty {
            if let Some(seg) = tp.path.segments.last() {
                let n = seg.ident.to_string();
                if matches!(
                    n.as_str(),
                    "Vec" | "Option" | "HashMap" | "BTreeMap" | "HashSet" | "BTreeSet"
                ) {
                    return Err(syn::Error::new_spanned(
                        field,
                        format!(
                            "`#[unique]` cannot be used on `{n}` — UNIQUE indexes \
                             only make sense on scalar fields"
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_no_base_field_collision(item: &ItemStruct) -> syn::Result<()> {
    let Fields::Named(named) = &item.fields else {
        return Ok(());
    };

    for field in &named.named {
        let name = field
            .ident
            .as_ref()
            .expect("named field has ident")
            .to_string();
        if matches!(name.as_str(), "created_at" | "updated_at" | "version") {
            return Err(syn::Error::new_spanned(
                field,
                format!(
                    "`{name}` is injected by `#[resource]` and must not be declared manually"
                ),
            ));
        }
    }

    Ok(())
}

// -------- DDL generation --------

fn generate_user_field_ddl(table: &str, item: &ItemStruct) -> syn::Result<String> {
    let Fields::Named(named) = &item.fields else {
        return Ok(String::new());
    };
    let mut out = String::new();
    for field in &named.named {
        let name = field.ident.as_ref().expect("named field has ident");
        let surql_type = rust_to_surql_type(&field.ty)
            .map_err(|e| syn::Error::new_spanned(&field.ty, e))?;
        out.push_str(&format!(
            "DEFINE FIELD IF NOT EXISTS {name} ON {table} TYPE {surql_type};\n"
        ));
    }
    Ok(out)
}

fn generate_base_field_ddl(table: &str) -> String {
    format!(
        "DEFINE FIELD IF NOT EXISTS created_at ON {table} TYPE datetime;\n\
         DEFINE FIELD IF NOT EXISTS updated_at ON {table} TYPE datetime;\n\
         DEFINE FIELD IF NOT EXISTS version    ON {table} TYPE int;\n"
    )
}

fn generate_unique_indexes(table: &str, item: &ItemStruct) -> String {
    let Fields::Named(named) = &item.fields else {
        return String::new();
    };
    let mut out = String::new();
    for field in &named.named {
        if field.attrs.iter().any(|a| a.path().is_ident("unique")) {
            let name = field.ident.as_ref().expect("named field has ident");
            out.push_str(&format!(
                "DEFINE INDEX IF NOT EXISTS {table}_{name} ON {table} \
                 FIELDS {name} UNIQUE;\n"
            ));
        }
    }
    out
}

fn rust_to_surql_type(ty: &Type) -> Result<String, String> {
    let Type::Path(tp) = ty else {
        return Err("only path types are supported (e.g. `String`, `Option<T>`)".into());
    };
    let seg = tp
        .path
        .segments
        .last()
        .ok_or_else(|| "empty type path".to_string())?;
    let name = seg.ident.to_string();

    match name.as_str() {
        "String" => Ok("string".into()),
        "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "usize" | "isize" => {
            Ok("int".into())
        }
        "f32" | "f64" => Ok("float".into()),
        "bool" => Ok("bool".into()),
        "Datetime" => Ok("datetime".into()),
        "Uuid" => Ok("uuid".into()),
        "Option" => {
            let inner = first_type_arg(seg)?;
            Ok(format!("option<{}>", rust_to_surql_type(inner)?))
        }
        "Vec" => {
            let inner = first_type_arg(seg)?;
            Ok(format!("array<{}>", rust_to_surql_type(inner)?))
        }
        other => Err(format!(
            "unsupported type `{other}` — supported: `String`, integer types, `f32`/`f64`, \
             `bool`, `Datetime`, `Uuid`, `Option<T>`, `Vec<T>`"
        )),
    }
}

fn first_type_arg(seg: &PathSegment) -> Result<&Type, String> {
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(format!("expected `<T>` after `{}`", seg.ident));
    };
    args.args
        .iter()
        .find_map(|a| {
            if let GenericArgument::Type(t) = a {
                Some(t)
            } else {
                None
            }
        })
        .ok_or_else(|| format!("missing type argument inside `<>` after `{}`", seg.ident))
}

// -------- struct rewriting --------

fn inject_base_fields(item: &mut ItemStruct) {
    let Fields::Named(named) = &mut item.fields else {
        return;
    };
    named.named.push(parse_quote! {
        pub created_at: ::nauka_core::resource::Datetime
    });
    named.named.push(parse_quote! {
        pub updated_at: ::nauka_core::resource::Datetime
    });
    named.named.push(parse_quote! {
        pub version: u64
    });
}

fn strip_macro_attrs(item: &mut ItemStruct) {
    let Fields::Named(named) = &mut item.fields else {
        return;
    };
    for field in &mut named.named {
        field
            .attrs
            .retain(|a| !a.path().is_ident("id") && !a.path().is_ident("unique"));
    }
}
