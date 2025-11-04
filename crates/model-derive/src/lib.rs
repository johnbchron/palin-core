//! Provides the derive macro for the [`Model`] trait.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
  Data, DeriveInput, Expr, Fields, Lit, Meta, Token,
  parse::{Parse, ParseStream},
  parse_macro_input,
  punctuated::Punctuated,
};

/// Derive macro for Model trait
#[proc_macro_derive(Model, attributes(model))]
pub fn derive_model(input: TokenStream) -> TokenStream {
  let input = parse_macro_input!(input as DeriveInput);

  match expand_model(&input) {
    Ok(tokens) => tokens.into(),
    Err(err) => err.to_compile_error().into(),
  }
}

struct ModelAttrs {
  table_name: String,
  indices:    Vec<Index>,
}

impl ModelAttrs {
  fn parse(input: &DeriveInput) -> syn::Result<Self> {
    let mut table_name = None;
    let mut indices = Vec::new();

    for attr in &input.attrs {
      if !attr.path().is_ident("model") {
        continue;
      }

      attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("table") {
          let value: Lit = meta.value()?.parse()?;
          if let Lit::Str(s) = value {
            table_name = Some(s.value());
          }
        } else if meta.path.is_ident("index") {
          let content;
          syn::parenthesized!(content in meta.input);
          indices.push(content.parse()?);
        } else {
          return Err(meta.error("unrecognized model attribute"));
        }
        Ok(())
      })?;
    }

    let table_name = table_name.ok_or_else(|| {
      syn::Error::new_spanned(
        input,
        "missing #[model(table = \"...\")] attribute",
      )
    })?;

    Ok(Self {
      table_name,
      indices,
    })
  }
}

struct FieldAttrs {
  id_field: syn::Ident,
}

impl FieldAttrs {
  fn parse(input: &DeriveInput) -> syn::Result<Self> {
    let fields = match &input.data {
      Data::Struct(data) => match &data.fields {
        Fields::Named(fields) => &fields.named,
        _ => {
          return Err(syn::Error::new_spanned(
            input,
            "Model can only be derived for structs with named fields",
          ));
        }
      },
      _ => {
        return Err(syn::Error::new_spanned(
          input,
          "Model can only be derived for structs",
        ));
      }
    };

    let mut id_field = None;

    for field in fields {
      let field_name = field.ident.as_ref().unwrap();

      for attr in &field.attrs {
        if !attr.path().is_ident("model") {
          continue;
        }

        attr.parse_nested_meta(|meta| {
          if meta.path.is_ident("id") {
            id_field = Some(field_name.clone());
          } else {
            return Err(meta.error("unrecognized field attribute"));
          }
          Ok(())
        })?;
      }
    }

    let id_field = id_field.ok_or_else(|| {
      syn::Error::new_spanned(input, "missing #[model(id)] field attribute")
    })?;

    Ok(Self { id_field })
  }
}

struct IndexInfo {
  variant:    syn::Ident,
  name:       String,
  definition: proc_macro2::TokenStream,
}

fn expand_model(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
  let struct_name = &input.ident;
  let model_attrs = ModelAttrs::parse(input)?;
  let field_attrs = FieldAttrs::parse(input)?;

  let index_selector_name = format_ident!("{}IndexSelector", struct_name);
  let indices = collect_indices(&model_attrs);

  let (enum_def, display_impl) = if indices.is_empty() {
    generate_empty_enum(&index_selector_name)
  } else {
    generate_enum(&index_selector_name, &indices)
  };

  let index_definitions: Vec<_> =
    indices.iter().map(|i| &i.definition).collect();
  let table_name = &model_attrs.table_name;
  let id_field = &field_attrs.id_field;

  Ok(quote! {
      #enum_def

      #display_impl

      impl model::Model for #struct_name {
          const TABLE_NAME: &'static str = #table_name;

          type IndexSelector = #index_selector_name;

          fn indices() -> &'static model::IndexRegistry<Self> {
              static REGISTRY: std::sync::OnceLock<model::IndexRegistry<#struct_name>> =
                  std::sync::OnceLock::new();

              REGISTRY.get_or_init(|| {
                  const DEFINITIONS: &[model::IndexDefinition<#struct_name>] = &[
                      #(#index_definitions),*
                  ];

                  model::IndexRegistry::new(DEFINITIONS)
              })
          }

          fn id(&self) -> RecordId<Self> {
              self.#id_field
          }
      }
  })
}

fn collect_indices(model_attrs: &ModelAttrs) -> Vec<IndexInfo> {
  let mut indices = Vec::new();

  for composite in &model_attrs.indices {
    let name = &composite.name;
    let unique = &composite.unique;
    let extract_fn = &composite.extract;
    indices.push(IndexInfo {
      variant:    format_ident!("{}", to_pascal_case(name)),
      name:       name.clone(),
      definition: quote! {
          model::IndexDefinition::new(#name, #unique, #extract_fn)
      },
    });
  }

  indices
}

fn generate_empty_enum(
  name: &syn::Ident,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
  let enum_def = quote! {
      #[derive(Debug, Clone, Copy)]
      #[allow(missing_docs)]
      pub enum #name {}
  };

  let display_impl = quote! {
      impl ::std::fmt::Display for #name {
          fn fmt(&self, _f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
              match *self {}
          }
      }
  };

  (enum_def, display_impl)
}

fn generate_enum(
  name: &syn::Ident,
  indices: &[IndexInfo],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
  let variants: Vec<_> = indices.iter().map(|i| &i.variant).collect();
  let names: Vec<_> = indices.iter().map(|i| &i.name).collect();

  let enum_def = quote! {
      #[derive(Debug, Clone, Copy)]
      #[allow(missing_docs)]
      pub enum #name {
          #(#variants),*
      }
  };

  let display_impl = quote! {
      impl ::std::fmt::Display for #name {
          fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
              match self {
                  #(Self::#variants => write!(f, #names)),*
              }
          }
      }
  };

  (enum_def, display_impl)
}

struct Index {
  name:    String,
  unique:  bool,
  extract: Expr,
}

impl Parse for Index {
  fn parse(input: ParseStream) -> syn::Result<Self> {
    let meta_items = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;

    let mut name = None;
    let mut unique = None;
    let mut extract = None;

    for meta in meta_items {
      match meta {
        Meta::Path(path) if path.is_ident("unique") => {
          unique = Some(true);
        }
        Meta::NameValue(pair) => {
          if pair.path.is_ident("name") {
            if let Expr::Lit(expr_lit) = &pair.value
              && let Lit::Str(s) = &expr_lit.lit
            {
              name = Some(s.value());
            }
          } else if pair.path.is_ident("extract") {
            extract = Some(pair.value.clone());
          }
        }
        Meta::Path(_) | Meta::List(_) => {
          return Err(input.error("unknown attribute item in composite index"));
        }
      }
    }

    Ok(Index {
      name:    name
        .ok_or_else(|| input.error("missing 'name' in composite index"))?,
      unique:  unique.unwrap_or(false),
      extract: extract
        .ok_or_else(|| input.error("missing 'extract' in composite index"))?,
    })
  }
}

fn to_pascal_case(s: &str) -> String {
  s.split('_')
    .map(|word| {
      let mut chars = word.chars();
      match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().chain(chars).collect(),
      }
    })
    .collect()
}
