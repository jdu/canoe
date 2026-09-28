//! The DSL's expression sub-language (used in `filter.predicate`,
//! `with_field.expression`, and `route`/`branch.when`) is intentionally a
//! SQL-flavored subset: bare field names, `=`/`<>`/`!=`/`<`/`<=`/`>`/`>=`,
//! `AND`/`OR`/`NOT`, arithmetic, and single-quoted string literals — the
//! same shape whether it ends up in a Spark SQL `WHERE`/`SELECT` clause or
//! evaluated inside a generated Python function.
//!
//! Spark SQL steps use expressions verbatim (they're already valid SQL).
//! Python steps need a small transpile: `=` -> `==`, `<>` -> `!=`, and the
//! boolean keywords lowercased, so the result is a valid Python expression
//! that can be evaluated with the record's fields as local names (bare
//! identifiers resolve because the record dict is passed as the `eval`
//! locals namespace — see codegen::python::eval_call).
//!
//! This is a lexer-level rewrite, not a full parser: it tracks single-quote
//! string boundaries so operators inside string literals are never touched,
//! but it does not validate the expression is otherwise well-formed —
//! malformed expressions surface as a Python/Spark syntax error at runtime,
//! not a DSL compile error. Documented simplification for the first pass.

pub fn to_python(sql_like: &str) -> String {
    let mut out = String::with_capacity(sql_like.len() + 8);
    let mut chars = sql_like.char_indices().peekable();
    let mut in_string = false;

    while let Some((i, c)) = chars.next() {
        if c == '\'' {
            in_string = !in_string;
            out.push(c);
            continue;
        }
        if in_string {
            out.push(c);
            continue;
        }

        let rest = &sql_like[i..];
        // Two-char comparison operators first, so a lone `=` rewrite below
        // never splits `<=`, `>=`, `==`, or `!=` apart.
        if rest.starts_with("==") || rest.starts_with("!=") || rest.starts_with("<=") || rest.starts_with(">=") {
            out.push_str(&rest[..2]);
            chars.next();
            continue;
        }
        if rest.starts_with("<>") {
            out.push_str("!=");
            chars.next();
            continue;
        }

        match c {
            '=' => out.push_str("=="),
            _ if is_word_boundary_keyword(&out, rest, "and") => {
                out.push_str("and");
                advance_by(&mut chars, 2);
            }
            _ if is_word_boundary_keyword(&out, rest, "or") => {
                out.push_str("or");
                advance_by(&mut chars, 1);
            }
            _ if is_word_boundary_keyword(&out, rest, "not") => {
                out.push_str("not");
                advance_by(&mut chars, 2);
            }
            _ => out.push(c),
        }
    }

    out
}

fn is_word_boundary_keyword(already_emitted: &str, rest: &str, word: &str) -> bool {
    if rest.len() < word.len() {
        return false;
    }
    let (candidate, after) = rest.split_at(word.len());
    if !candidate.eq_ignore_ascii_case(word) {
        return false;
    }
    let preceded_by_ident_char = already_emitted
        .chars()
        .next_back()
        .is_some_and(|c| c.is_alphanumeric() || c == '_');
    let followed_by_ident_char = after
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_');
    !preceded_by_ident_char && !followed_by_ident_char
}

fn advance_by(chars: &mut std::iter::Peekable<std::str::CharIndices>, n: usize) {
    for _ in 0..n {
        chars.next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transpiles_common_operators() {
        assert_eq!(to_python("region = 'US'"), "region == 'US'");
        assert_eq!(to_python("a <> b"), "a != b");
        assert_eq!(to_python("a = 1 AND b = 2"), "a == 1 and b == 2");
        assert_eq!(to_python("NOT (a = 1)"), "not (a == 1)");
    }

    #[test]
    fn leaves_multi_char_comparisons_intact() {
        assert_eq!(to_python("amount <= 10"), "amount <= 10");
        assert_eq!(to_python("amount >= 10"), "amount >= 10");
        assert_eq!(to_python("amount != 10"), "amount != 10");
        assert_eq!(to_python("amount == 10"), "amount == 10");
    }

    #[test]
    fn leaves_string_literals_untouched() {
        assert_eq!(to_python("status = 'A=B AND C'"), "status == 'A=B AND C'");
    }

    #[test]
    fn does_not_mangle_substrings_of_keywords() {
        assert_eq!(to_python("category = 'android'"), "category == 'android'");
        assert_eq!(to_python("note = 'organic'"), "note == 'organic'");
        assert_eq!(to_python("brand = 'x'"), "brand == 'x'");
    }
}
