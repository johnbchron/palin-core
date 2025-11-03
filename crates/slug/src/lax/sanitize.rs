//! Lax slugify implementation.

use deunicode::AsciiChars;

/// Convert any unicode string to "lax slug" (useful for nix store paths)
///
/// "Lax slugs" can contain any combination of characters from the regex set
/// `[-.+_0-9a-zA-Z]`, which is:
/// - a-z (lowercase)
/// - A-Z (uppercase)
/// - 0-9 (numbers)
/// - '+' (plus)
/// - '.' (dot)
/// - '_' (underscore)
/// - '-' (dash)
///
/// Any other characters will be replaced with a single dash.
pub fn lax_slugify(s: &str) -> String {
  let mut slug = String::with_capacity(s.len());

  // deunicode non-ascii and replace with ascii substrings, then iterate on
  // the characters in those substrings
  let iter = s.ascii_chars().flatten().flat_map(|s| s.chars());

  for c in iter {
    push_ascii_char(&mut slug, c as u8);
  }

  // we likely reserved more space than needed
  slug.shrink_to_fit();
  slug
}

fn push_ascii_char(slug: &mut String, x: u8) {
  let char = match x {
    c @ (b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'+' | b'.' | b'_') => {
      c.into()
    }
    _ => '-',
  };
  slug.push(char);
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_lax_slugify_basic() {
    // Basic ASCII input
    assert_eq!(lax_slugify("hello-world"), "hello-world");
    assert_eq!(lax_slugify("Hello World"), "Hello-World");
    assert_eq!(lax_slugify("rust+rocks"), "rust+rocks");
    // trailing dashes are rpeserved
    assert_eq!(lax_slugify("hello-world-"), "hello-world-");
  }

  #[test]
  fn test_lax_slugify_unicode() {
    // Unicode characters that should be replaced with dashes
    assert_eq!(lax_slugify("你好世界"), "Ni-Hao-Shi-Jie");
    assert_eq!(lax_slugify("こんにちは"), "konnitiha");
    assert_eq!(lax_slugify("¡Hola!"), "-Hola-");
  }

  #[test]
  fn test_lax_slugify_mixed() {
    // Mixed ASCII and Unicode
    assert_eq!(lax_slugify("rust编程语言"), "rustBian-Cheng-Yu-Yan");
    assert_eq!(lax_slugify("Lörem Ipsum"), "Lorem-Ipsum");
    assert_eq!(lax_slugify("foo@bar.com"), "foo-bar.com");
  }

  #[test]
  fn test_lax_slugify_special_characters() {
    // Input with special characters
    assert_eq!(lax_slugify("foo_bar.baz+qux"), "foo_bar.baz+qux");
    assert_eq!(lax_slugify("foo/bar\\baz"), "foo-bar-baz");
    assert_eq!(lax_slugify("hello*world"), "hello-world");
  }

  #[test]
  fn test_lax_slugify_edge_cases() {
    // Edge cases
    assert_eq!(lax_slugify(""), "");
    assert_eq!(lax_slugify("  "), "--");
    assert_eq!(lax_slugify("..."), "...");
    assert_eq!(lax_slugify("a"), "a");
    assert_eq!(lax_slugify("-_-"), "-_-");
  }
}
