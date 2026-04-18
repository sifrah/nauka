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
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::{
    parse_macro_input, parse_quote, Field, Fields, GenericArgument, Ident, Item, ItemStruct,
    LitStr, PathArguments, PathSegment, Token, Type,
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

/// Declare a SurrealDB `DEFINE ACCESS TYPE RECORD` statement alongside
/// a resource. Wiring identical to `#[resource]`: the macro emits a
/// `&'static AccessDescriptor` and registers it in
/// `ALL_ACCESS_DEFS` via `linkme::distributed_slice`, so the binary
/// picks it up at link time.
///
/// Usage — applied to the struct whose table the access authenticates:
///
/// ```ignore
/// #[resource(table = "user", scope = "cluster")]
/// #[access(
///     name = "user",
///     type = "record",
///     signup = "...",
///     signin = "...",
///     jwt_duration = "1h",
///     session_duration = "24h",
/// )]
/// pub struct User { ... }
/// ```
///
/// The `type` argument is currently required and must be `"record"`
/// — reserved for when `DEFINE ACCESS TYPE JWT` (OIDC / SSO) lands.
#[proc_macro_attribute]
pub fn access(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AccessArgs);
    let item = parse_macro_input!(input as Item);

    match expand_access(args, item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// -------- argument parsing --------

struct ResourceArgs {
    table: LitStr,
    scope: LitStr,
    /// `cascade_delete = "field1, field2"` — local Ref-typed fields
    /// whose referenced records are deleted when this resource is
    /// deleted.
    cascade_delete: Option<LitStr>,
    /// `restrict_delete = "table:field, table2:field2"` — external
    /// (table, field) pairs. Deleting this resource fails if any of
    /// those fields point at the deleted id.
    restrict_delete: Option<LitStr>,
    /// `set_null_on_delete = "table:field, …"` — external fields
    /// that get set to `NONE` when this resource is deleted. Target
    /// field MUST be `Option<Ref<_>>` at the DDL layer, but the
    /// macro cannot verify cross-crate.
    set_null_on_delete: Option<LitStr>,
}

impl Parse for ResourceArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut table: Option<LitStr> = None;
        let mut scope: Option<LitStr> = None;
        let mut cascade_delete: Option<LitStr> = None;
        let mut restrict_delete: Option<LitStr> = None;
        let mut set_null_on_delete: Option<LitStr> = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;
            let slot = match key.to_string().as_str() {
                "table" => &mut table,
                "scope" => &mut scope,
                "cascade_delete" => &mut cascade_delete,
                "restrict_delete" => &mut restrict_delete,
                "set_null_on_delete" => &mut set_null_on_delete,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown attribute key `{other}` (allowed: `table`, `scope`, \
                             `cascade_delete`, `restrict_delete`, `set_null_on_delete`)"
                        ),
                    ));
                }
            };
            if slot.is_some() {
                return Err(syn::Error::new(
                    key.span(),
                    format!("`{}` specified more than once", key),
                ));
            }
            *slot = Some(value);
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

        Ok(ResourceArgs {
            table,
            scope,
            cascade_delete,
            restrict_delete,
            set_null_on_delete,
        })
    }
}

/// Comma-separated list of `"table:field"` pairs. Each entry reads
/// as the external `(table, field)` tuple the `on_delete` rule
/// applies to.
fn parse_table_field_pairs(lit: &LitStr) -> syn::Result<Vec<(String, String)>> {
    let raw = lit.value();
    let mut out = Vec::new();
    for (i, part) in raw.split(',').map(|s| s.trim()).enumerate() {
        if part.is_empty() {
            continue;
        }
        let (table, field) = part.split_once(':').ok_or_else(|| {
            syn::Error::new(
                lit.span(),
                format!(
                    "entry #{} `{part}` is not in `table:field` form — \
                     e.g. `\"vm:host, backup:vm\"`",
                    i + 1
                ),
            )
        })?;
        let table = table.trim();
        let field = field.trim();
        if table.is_empty() || field.is_empty() {
            return Err(syn::Error::new(
                lit.span(),
                format!("entry #{} `{part}` has an empty table or field", i + 1),
            ));
        }
        out.push((table.to_string(), field.to_string()));
    }
    Ok(out)
}

/// Comma-separated list of local field names.
fn parse_field_list(lit: &LitStr) -> syn::Result<Vec<String>> {
    Ok(lit
        .value()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
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

    let cascade_fields = match &args.cascade_delete {
        Some(lit) => {
            let names = parse_field_list(lit)?;
            validate_cascade_targets(&item, &names, lit)?;
            names
        }
        None => Vec::new(),
    };
    let restrict_pairs = match &args.restrict_delete {
        Some(lit) => parse_table_field_pairs(lit)?,
        None => Vec::new(),
    };
    let set_null_pairs = match &args.set_null_on_delete {
        Some(lit) => parse_table_field_pairs(lit)?,
        None => Vec::new(),
    };

    let user_ddl = generate_user_field_ddl(&table, &item)?;
    let unique_ddl = generate_unique_indexes(&table, &item);
    let base_ddl = generate_base_field_ddl(&table);
    let event_ddl =
        generate_on_delete_event(&table, &cascade_fields, &restrict_pairs, &set_null_pairs);
    let full_ddl = format!(
        "DEFINE TABLE IF NOT EXISTS {table} SCHEMAFULL;\n{user_ddl}{base_ddl}{unique_ddl}{event_ddl}"
    );

    // Build the SET-clause expression list *before* adding base fields
    // so we can append them (with their own Datetime/u64 emitters)
    // explicitly and keep a stable ordering.
    let user_set_exprs = build_set_exprs(&item)?;
    let base_set_exprs = build_base_set_exprs();

    inject_base_fields(&mut item);
    strip_macro_attrs(&mut item);

    let struct_name = item.ident.clone();
    let scope_path = match scope {
        ParsedScope::Local => quote!(::nauka_core::resource::Scope::Local),
        ParsedScope::Cluster => quote!(::nauka_core::resource::Scope::Cluster),
    };

    let static_name = format_ident!("__NAUKA_RES_{}", struct_name.to_string().to_uppercase());

    let all_set_exprs = user_set_exprs
        .into_iter()
        .chain(base_set_exprs)
        .collect::<Vec<_>>();

    let create_body = build_create_body(&table, &id_field_name, &all_set_exprs);
    let update_body = build_update_body(&table, &id_field_name, &all_set_exprs);

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

        impl ::nauka_core::resource::ResourceOps for #struct_name {
            fn create_query(&self) -> ::std::string::String {
                #create_body
            }

            fn update_query(&self) -> ::std::string::String {
                #update_body
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

/// Produce `Vec<TokenStream>` where each element evaluates at
/// runtime to the string `"field_name = surql_literal"` for one user
/// field of the struct.
fn build_set_exprs(item: &ItemStruct) -> syn::Result<Vec<TokenStream2>> {
    let Fields::Named(named) = &item.fields else {
        return Ok(Vec::new());
    };
    named
        .named
        .iter()
        .map(|f| {
            let name = f.ident.as_ref().expect("named field has ident");
            let name_str = name.to_string();
            let value_expr = emit_literal_expr(&f.ty, &quote!(&self.#name))
                .map_err(|e| syn::Error::new_spanned(&f.ty, e))?;
            Ok(quote! {
                format!("{} = {}", #name_str, { #value_expr })
            })
        })
        .collect()
}

/// Emitters for the three base fields the macro injected. These
/// know their types directly and do not need Rust→SurrealQL
/// inference.
fn build_base_set_exprs() -> Vec<TokenStream2> {
    vec![
        quote! {
            format!(
                "created_at = <datetime>\"{}\"",
                (&self.created_at).to_string()
            )
        },
        quote! {
            format!(
                "updated_at = <datetime>\"{}\"",
                (&self.updated_at).to_string()
            )
        },
        quote! {
            format!("version = {}", self.version as i64)
        },
    ]
}

fn build_create_body(table: &str, id_field: &Ident, set_exprs: &[TokenStream2]) -> TokenStream2 {
    quote! {
        let set_parts: ::std::vec::Vec<::std::string::String> =
            vec![ #(#set_exprs),* ];
        let id_str = self.#id_field.to_string();
        format!(
            "CREATE {}:\u{27E8}{}\u{27E9} SET {}",
            #table,
            ::nauka_core::resource::escape_record_id(&id_str),
            set_parts.join(", "),
        )
    }
}

fn build_update_body(table: &str, id_field: &Ident, set_exprs: &[TokenStream2]) -> TokenStream2 {
    quote! {
        let set_parts: ::std::vec::Vec<::std::string::String> =
            vec![ #(#set_exprs),* ];
        let id_str = self.#id_field.to_string();
        format!(
            "UPDATE {}:\u{27E8}{}\u{27E9} SET {}",
            #table,
            ::nauka_core::resource::escape_record_id(&id_str),
            set_parts.join(", "),
        )
    }
}

/// Produce a `TokenStream` that, when spliced into generated code,
/// evaluates at runtime to a `String` holding the SurrealQL literal
/// for the value borrowed by `access`.
///
/// The caller passes `access` as a `&T` expression — this keeps the
/// recursive `Option`/`Vec` cases uniform (the inner binding is also
/// a `&T`).
///
/// Supports the same type closed-set as [`rust_to_surql_type`] —
/// unsupported types are rejected at macro expansion so the mismatch
/// shows up at compile time, not at runtime.
fn emit_literal_expr(ty: &Type, access: &TokenStream2) -> Result<TokenStream2, String> {
    let Type::Path(tp) = ty else {
        return Err("only path types are supported in `#[resource]` fields".into());
    };
    let seg = tp
        .path
        .segments
        .last()
        .ok_or_else(|| "empty type path".to_string())?;
    let name = seg.ident.to_string();

    // All branches parenthesize `#access` because callers may splice
    // in an expression like `&self.field`, and Rust's precedence
    // makes `&self.field.method()` parse as `&(self.field.method())`.
    match name.as_str() {
        "String" => Ok(quote! {
            format!("\"{}\"", ::nauka_core::resource::escape_surql_string((#access)))
        }),
        "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "usize" | "isize" => {
            Ok(quote! { (*(#access) as i64).to_string() })
        }
        "f32" | "f64" => Ok(quote! { (*(#access) as f64).to_string() }),
        "bool" => Ok(quote! { (#access).to_string() }),
        "Datetime" => Ok(quote! {
            format!("<datetime>\"{}\"", (#access).to_string())
        }),
        "Uuid" => Ok(quote! {
            format!("<uuid>\"{}\"", (#access).to_string())
        }),
        "Ref" => {
            // Emit a bare record literal `{table}:⟨{id}⟩`. Record-id
            // characters are escaped against bracket-escape injection.
            let inner = first_type_arg(seg).map_err(|e| e.to_string())?;
            let target_ident = ref_target_ident(inner)?;
            let target_table = pascal_to_snake(&target_ident);
            Ok(quote! {
                format!(
                    "{}:\u{27E8}{}\u{27E9}",
                    #target_table,
                    ::nauka_core::resource::escape_record_id((#access).id())
                )
            })
        }
        "Option" => {
            let inner = first_type_arg(seg).map_err(|e| e.to_string())?;
            let inner_expr = emit_literal_expr(inner, &quote!(v))?;
            Ok(quote! {
                match (#access) {
                    ::std::option::Option::Some(v) => { #inner_expr },
                    ::std::option::Option::None => "NONE".to_string(),
                }
            })
        }
        "Vec" => {
            let inner = first_type_arg(seg).map_err(|e| e.to_string())?;
            let inner_expr = emit_literal_expr(inner, &quote!(v))?;
            Ok(quote! {
                {
                    let items: ::std::vec::Vec<::std::string::String> =
                        (#access).iter().map(|v| { #inner_expr }).collect();
                    format!("[{}]", items.join(","))
                }
            })
        }
        other => Err(format!(
            "unsupported type `{other}` — supported: `String`, integer types, `f32`/`f64`, \
             `bool`, `Datetime`, `Uuid`, `Option<T>`, `Vec<T>`"
        )),
    }
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
                format!("`{name}` is injected by `#[resource]` and must not be declared manually"),
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
        let surql_type =
            rust_to_surql_type(&field.ty).map_err(|e| syn::Error::new_spanned(&field.ty, e))?;
        let assert_clause = extract_assert(field)?;
        out.push_str(&format!(
            "DEFINE FIELD IF NOT EXISTS {name} ON {table} TYPE {surql_type}{assert_clause};\n"
        ));
    }
    Ok(out)
}

/// Pull the inner SurrealQL predicate out of a `#[assert("…")]`
/// field attribute. At most one `#[assert]` per field.
fn extract_assert(field: &Field) -> syn::Result<String> {
    let mut found: Option<LitStr> = None;
    for attr in &field.attrs {
        if !attr.path().is_ident("assert") {
            continue;
        }
        if found.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "`#[assert]` may appear at most once per field",
            ));
        }
        let lit: LitStr = attr.parse_args().map_err(|_| {
            syn::Error::new_spanned(
                attr,
                "`#[assert]` takes one string literal — e.g. `#[assert(\"$value > 0\")]`",
            )
        })?;
        found = Some(lit);
    }
    Ok(match found {
        Some(lit) => format!(" ASSERT {}", lit.value()),
        None => String::new(),
    })
}

fn generate_base_field_ddl(table: &str) -> String {
    format!(
        "DEFINE FIELD IF NOT EXISTS created_at ON {table} TYPE datetime;\n\
         DEFINE FIELD IF NOT EXISTS updated_at ON {table} TYPE datetime;\n\
         DEFINE FIELD IF NOT EXISTS version    ON {table} TYPE int;\n"
    )
}

/// Check that each name in `cascade_fields` is a real field of the
/// struct whose type is `Ref<T>`, `Vec<Ref<T>>`, or `Option<Ref<T>>`.
/// Cascading on a non-reference field makes no sense and would emit
/// invalid SurrealQL.
fn validate_cascade_targets(
    item: &ItemStruct,
    cascade_fields: &[String],
    lit: &LitStr,
) -> syn::Result<()> {
    let Fields::Named(named) = &item.fields else {
        return Ok(());
    };

    for name in cascade_fields {
        let field = named
            .named
            .iter()
            .find(|f| f.ident.as_ref().map(|i| i == name).unwrap_or(false))
            .ok_or_else(|| {
                syn::Error::new(
                    lit.span(),
                    format!(
                        "`cascade_delete` names field `{name}` which does not exist \
                         on this struct"
                    ),
                )
            })?;

        if !is_ref_or_container_of_ref(&field.ty) {
            return Err(syn::Error::new(
                lit.span(),
                format!(
                    "`cascade_delete = \"{name}\"` targets a field that is not a \
                     `Ref<T>` / `Vec<Ref<T>>` / `Option<Ref<T>>` — cascading only \
                     makes sense on reference fields"
                ),
            ));
        }
    }

    Ok(())
}

/// `Ref<T>` / `Option<Ref<T>>` / `Vec<Ref<T>>`.
fn is_ref_or_container_of_ref(ty: &Type) -> bool {
    let Type::Path(tp) = ty else {
        return false;
    };
    let Some(seg) = tp.path.segments.last() else {
        return false;
    };
    match seg.ident.to_string().as_str() {
        "Ref" => true,
        "Option" | "Vec" => first_type_arg(seg)
            .map(is_ref_or_container_of_ref)
            .unwrap_or(false),
        _ => false,
    }
}

fn generate_on_delete_event(
    table: &str,
    cascade_fields: &[String],
    restrict_pairs: &[(String, String)],
    set_null_pairs: &[(String, String)],
) -> String {
    if cascade_fields.is_empty() && restrict_pairs.is_empty() && set_null_pairs.is_empty() {
        return String::new();
    }

    let mut body = String::new();

    // --- cascade: delete owned children ---
    for field in cascade_fields {
        body.push_str(&format!("    DELETE $before.{field};\n"));
    }

    // --- restrict: fail if anything still references us ---
    for (other_table, other_field) in restrict_pairs {
        body.push_str(&format!(
            "    IF (SELECT VALUE count() FROM {other_table} \
             WHERE {other_field} = $before.id GROUP ALL)[0] > 0 {{\n\
             \x20\x20\x20\x20\x20\x20\x20\x20THROW \"cannot delete {table}: still referenced \
             by {other_table}.{other_field}\";\n\
             \x20\x20\x20\x20}};\n"
        ));
    }

    // --- set_null: null out back-references ---
    for (other_table, other_field) in set_null_pairs {
        body.push_str(&format!(
            "    UPDATE {other_table} SET {other_field} = NONE \
             WHERE {other_field} = $before.id;\n"
        ));
    }

    format!(
        "DEFINE EVENT IF NOT EXISTS {table}_on_delete ON {table} \
         WHEN $event = \"DELETE\" THEN {{\n{body}}};\n"
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
        "Ref" => {
            // `Ref<TargetType>` → `record<target_type>`. The target
            // type's PascalCase → snake_case mapping is what links
            // the reference to the other resource's table, without
            // the author writing the name twice.
            let inner = first_type_arg(seg)?;
            let target_ident = ref_target_ident(inner)?;
            Ok(format!("record<{}>", pascal_to_snake(&target_ident)))
        }
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
             `bool`, `Datetime`, `Uuid`, `Ref<T>`, `Option<T>`, `Vec<T>`"
        )),
    }
}

/// Given the inner type of a `Ref<T>`, extract the last path segment
/// as a string — e.g. `crate::Hypervisor` → `"Hypervisor"`.
fn ref_target_ident(inner: &Type) -> Result<String, String> {
    let Type::Path(tp) = inner else {
        return Err("`Ref<T>` requires a path type as the target (e.g. `Ref<Hypervisor>`)".into());
    };
    tp.path
        .segments
        .last()
        .map(|s| s.ident.to_string())
        .ok_or_else(|| "empty path inside `Ref<…>`".to_string())
}

/// PascalCase → snake_case — same rule as
/// `nauka_core::resource::pascal_to_snake`. Duplicated here so the
/// macro can run at expansion time without cross-crate coupling at
/// build time.
fn pascal_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for (i, c) in s.char_indices() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
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
        field.attrs.retain(|a| {
            !a.path().is_ident("id") && !a.path().is_ident("unique") && !a.path().is_ident("assert")
        });
    }
}

// =========================================================================
// #[access] — SurrealDB DEFINE ACCESS TYPE RECORD
// =========================================================================

struct AccessArgs {
    name: LitStr,
    ty: LitStr,
    signup: Option<LitStr>,
    signin: LitStr,
    jwt_duration: LitStr,
    session_duration: LitStr,
}

impl Parse for AccessArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name: Option<LitStr> = None;
        let mut ty: Option<LitStr> = None;
        let mut signup: Option<LitStr> = None;
        let mut signin: Option<LitStr> = None;
        let mut jwt_duration: Option<LitStr> = None;
        let mut session_duration: Option<LitStr> = None;

        while !input.is_empty() {
            // `parse_any` accepts the reserved keyword `type` as an
            // identifier — the epic specifies `type = "record"` so we
            // can't rename the key, and `Ident::parse` rejects all
            // reserved words by default.
            let key: Ident = input.call(Ident::parse_any)?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;
            let key_s = key.to_string();
            let slot = match key_s.as_str() {
                "name" => &mut name,
                "type" => &mut ty,
                "signup" => &mut signup,
                "signin" => &mut signin,
                "jwt_duration" => &mut jwt_duration,
                "session_duration" => &mut session_duration,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown `#[access]` key `{other}` (allowed: `name`, `type`, \
                             `signup`, `signin`, `jwt_duration`, `session_duration`)"
                        ),
                    ));
                }
            };
            if slot.is_some() {
                return Err(syn::Error::new(
                    key.span(),
                    format!("`{key_s}` specified more than once"),
                ));
            }
            *slot = Some(value);
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        let name = name.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[access]` requires `name = \"…\"`",
            )
        })?;
        let ty = ty.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[access]` requires `type = \"record\"` — only `record` is supported today",
            )
        })?;
        if ty.value() != "record" {
            return Err(syn::Error::new(
                ty.span(),
                format!(
                    "`#[access]` type `{}` is not supported — only `\"record\"` today; \
                     `\"jwt\"` is reserved for OIDC/SSO (IAM epic post-MVP)",
                    ty.value()
                ),
            ));
        }
        let signin = signin.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[access]` requires `signin = \"…\"` — the SurrealQL clause that \
                 authenticates an existing user",
            )
        })?;
        let jwt_duration = jwt_duration.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[access]` requires `jwt_duration = \"…\"` — e.g. `\"1h\"`",
            )
        })?;
        let session_duration = session_duration.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "`#[access]` requires `session_duration = \"…\"` — e.g. `\"24h\"`",
            )
        })?;

        validate_access_name(&name)?;
        validate_duration(&jwt_duration, "jwt_duration")?;
        validate_duration(&session_duration, "session_duration")?;

        Ok(AccessArgs {
            name,
            ty,
            signup,
            signin,
            jwt_duration,
            session_duration,
        })
    }
}

fn validate_access_name(lit: &LitStr) -> syn::Result<()> {
    let v = lit.value();
    if v.is_empty() {
        return Err(syn::Error::new(lit.span(), "access name cannot be empty"));
    }
    let bytes = v.as_bytes();
    if !bytes[0].is_ascii_alphabetic() {
        return Err(syn::Error::new(
            lit.span(),
            format!("access name `{v}` must start with an ASCII letter"),
        ));
    }
    for &b in bytes {
        let ok = b.is_ascii_alphanumeric() || b == b'_';
        if !ok {
            return Err(syn::Error::new(
                lit.span(),
                format!(
                    "access name `{v}` must contain only ASCII letters, digits, and `_` \
                     — it is spliced directly into SurrealQL"
                ),
            ));
        }
    }
    Ok(())
}

/// `1h`, `24h`, `15m`, `30s`, `7d`, `1w`, `4y`. Matches SurrealDB's
/// duration-literal syntax for `DURATION FOR TOKEN … / FOR SESSION …`.
fn validate_duration(lit: &LitStr, field: &str) -> syn::Result<()> {
    let v = lit.value();
    if v.is_empty() {
        return Err(syn::Error::new(
            lit.span(),
            format!("`{field}` cannot be empty — e.g. `\"1h\"`"),
        ));
    }
    let bytes = v.as_bytes();
    let last = *bytes.last().unwrap();
    if !matches!(last, b's' | b'm' | b'h' | b'd' | b'w' | b'y') {
        return Err(syn::Error::new(
            lit.span(),
            format!(
                "`{field}` must be a SurrealDB duration like `\"1h\"` or `\"24h\"` — \
                 unit suffix must be one of `s`, `m`, `h`, `d`, `w`, `y`"
            ),
        ));
    }
    let digits = &bytes[..bytes.len() - 1];
    if digits.is_empty() || !digits.iter().all(|b| b.is_ascii_digit()) {
        return Err(syn::Error::new(
            lit.span(),
            format!("`{field}` value `{v}` is not a positive integer followed by a unit"),
        ));
    }
    Ok(())
}

fn expand_access(args: AccessArgs, item: Item) -> syn::Result<TokenStream2> {
    // The item is re-emitted unchanged — the attribute only adds a side
    // effect (registering into ALL_ACCESS_DEFS). Unlike `#[resource]`,
    // `#[access]` carries no struct-shape invariants of its own.
    let name = args.name.value();
    let signup_clause = match args.signup {
        Some(ref s) => format!(" SIGNUP ( {} )", s.value()),
        None => String::new(),
    };
    let signin_clause = format!(" SIGNIN ( {} )", args.signin.value());
    let ddl = format!(
        "DEFINE ACCESS IF NOT EXISTS {name} ON DATABASE TYPE RECORD{signup_clause}{signin_clause} \
         DURATION FOR TOKEN {jwt}, FOR SESSION {sess};",
        jwt = args.jwt_duration.value(),
        sess = args.session_duration.value(),
    );

    let static_name = format_ident!("__NAUKA_ACCESS_{}", name.to_uppercase());
    let _ = args.ty; // reserved for future types (jwt, …)

    Ok(quote! {
        #item

        #[::nauka_core::resource::__macro_support::linkme::distributed_slice(
            ::nauka_core::resource::ALL_ACCESS_DEFS
        )]
        #[linkme(crate = ::nauka_core::resource::__macro_support::linkme)]
        #[allow(non_upper_case_globals)]
        static #static_name: &::nauka_core::resource::AccessDescriptor =
            &::nauka_core::resource::AccessDescriptor {
                name: #name,
                ddl: #ddl,
            };
    })
}
