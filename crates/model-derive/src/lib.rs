//! Provides the derive macro for the `Model` trait.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
  Data, DeriveInput, Expr, Fields, Lit, MetaNameValue, Token,
  parse::{Parse, ParseStream},
  parse_macro_input,
  punctuated::Punctuated,
};

/// Derive macro for Model trait
#[proc_macro_derive(Model, attributes(model))]
pub fn derive_model(input: TokenStream) -> TokenStream {
  let input = parse_macro_input!(input as DeriveInput);

  match expand_model(input) {
    Ok(tokens) => tokens.into(),
    Err(err) => err.to_compile_error().into(),
  }
}

fn expand_model(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
  let struct_name = &input.ident;

  // Parse attributes
  let mut table_name = None;
  let mut composite_indices = Vec::new();
  let mut composite_uniques = Vec::new();

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
        Ok(())
      } else if meta.path.is_ident("composite_index") {
        let content;
        syn::parenthesized!(content in meta.input);
        let composite: CompositeIndex = content.parse()?;
        composite_indices.push(composite);
        Ok(())
      } else if meta.path.is_ident("composite_unique") {
        let content;
        syn::parenthesized!(content in meta.input);
        let composite: CompositeIndex = content.parse()?;
        composite_uniques.push(composite);
        Ok(())
      } else {
        Err(meta.error("unrecognized model attribute"))
      }
    })?;
  }

  let table_name = table_name.ok_or_else(|| {
    syn::Error::new_spanned(
      &input,
      "missing #[model(table = \"...\")] attribute",
    )
  })?;

  // Parse fields
  let fields = match &input.data {
    Data::Struct(data) => match &data.fields {
      Fields::Named(fields) => &fields.named,
      _ => {
        return Err(syn::Error::new_spanned(
          &input,
          "Model can only be derived for structs with named fields",
        ));
      }
    },
    _ => {
      return Err(syn::Error::new_spanned(
        &input,
        "Model can only be derived for structs",
      ));
    }
  };

  let mut id_field = None;
  let mut unique_fields = Vec::new();
  let mut index_fields = Vec::new();

  for field in fields {
    let field_name = field.ident.as_ref().unwrap();

    for attr in &field.attrs {
      if !attr.path().is_ident("model") {
        continue;
      }

      attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("id") {
          id_field = Some(field_name.clone());
          Ok(())
        } else if meta.path.is_ident("unique") {
          unique_fields.push(field_name.clone());
          Ok(())
        } else if meta.path.is_ident("index") {
          index_fields.push(field_name.clone());
          Ok(())
        } else {
          Err(meta.error("unrecognized field attribute"))
        }
      })?;
    }
  }

  let id_field = id_field.ok_or_else(|| {
    syn::Error::new_spanned(&input, "missing #[model(id)] field attribute")
  })?;

  // Generate IndexSelector enum
  let mut selector_variants = Vec::new();
  let mut selector_names = Vec::new();
  let mut index_definitions = Vec::new();

  // Add unique field indices
  for field in &unique_fields {
    let variant_name = to_pascal_case(&field.to_string());
    let variant_ident = format_ident!("{}", variant_name);
    let field_name_snake = field.to_string();

    selector_variants.push(quote! { #variant_ident });
    selector_names.push(field_name_snake.clone());

    index_definitions.push(quote! {
        ::model::IndexDefinition::new(
            #field_name_snake,
            true,
            |m: &#struct_name| vec![model::IndexValue::from(m.#field.clone())]
        )
    });
  }

  // Add non-unique field indices
  for field in &index_fields {
    let variant_name = to_pascal_case(&field.to_string());
    let variant_ident = format_ident!("{}", variant_name);
    let field_name_snake = field.to_string();

    selector_variants.push(quote! { #variant_ident });
    selector_names.push(field_name_snake.clone());

    index_definitions.push(quote! {
        model::IndexDefinition::new(
            #field_name_snake,
            false,
            |m: &#struct_name| vec![model::IndexValue::from(m.#field.clone())]
        )
    });
  }

  // Add composite indices
  for composite in &composite_indices {
    let name = &composite.name;
    let variant_name = to_pascal_case(name);
    let variant_ident = format_ident!("{}", variant_name);
    let extract_fn = &composite.extract;

    selector_variants.push(quote! { #variant_ident });
    selector_names.push(name.clone());

    index_definitions.push(quote! {
        model::IndexDefinition::new(
            #name,
            false,
            #extract_fn
        )
    });
  }

  // Add composite unique indices
  for composite in &composite_uniques {
    let name = &composite.name;
    let variant_name = to_pascal_case(name);
    let variant_ident = format_ident!("{}", variant_name);
    let extract_fn = &composite.extract;

    selector_variants.push(quote! { #variant_ident });
    selector_names.push(name.clone());

    index_definitions.push(quote! {
        model::IndexDefinition::new(
            #name,
            true,
            #extract_fn
        )
    });
  }

  let index_selector_name = format_ident!("{}IndexSelector", struct_name);

  // Handle empty enum case
  let (enum_def, display_impl) = if selector_variants.is_empty() {
    (
      quote! {
          #[derive(Debug, Clone, Copy)]
          #[allow(missing_docs)]
          pub enum #index_selector_name {}
      },
      quote! {
          impl ::std::fmt::Display for #index_selector_name {
              fn fmt(&self, _f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                  match *self {}
              }
          }
      },
    )
  } else {
    (
      quote! {
          #[derive(Debug, Clone, Copy)]
          #[allow(missing_docs)]
          pub enum #index_selector_name {
              #(#selector_variants),*
          }
      },
      quote! {
          impl ::std::fmt::Display for #index_selector_name {
              fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                  match self {
                      #(
                          Self::#selector_variants => {
                              write!(f, #selector_names)
                          }
                      ),*
                  }
              }
          }
      },
    )
  };

  let expanded = quote! {
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
  };

  Ok(expanded)
}

struct CompositeIndex {
  name:    String,
  extract: Expr,
}

impl Parse for CompositeIndex {
  fn parse(input: ParseStream) -> syn::Result<Self> {
    let mut name = None;
    let mut extract = None;

    let pairs =
      Punctuated::<MetaNameValue, Token![,]>::parse_terminated(input)?;

    for pair in pairs {
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

    Ok(CompositeIndex {
      name:    name
        .ok_or_else(|| input.error("missing 'name' in composite index"))?,
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
