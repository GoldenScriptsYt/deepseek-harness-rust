//! Bounded HTML-to-markdown conversion for the model-facing fetch result.
//!
//! The tokenizer keeps the conversion linear in the source length and drops
//! script, style, and hidden content wholesale. Fidelity is deliberately below
//! the browser DOM converter the TypeScript Host uses: tables lose their
//! alignment separator row and inline styling is not preserved.

use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};
use std::cell::RefCell;

/// Elements whose content never reaches the model.
const DROPPED: &[&str] = &[
    "head", "script", "style", "noscript", "template", "iframe", "object", "embed", "svg",
];

/// Elements that separate blocks of text.
const BLOCK: &[&str] = &[
    "p",
    "div",
    "section",
    "article",
    "header",
    "footer",
    "main",
    "aside",
    "nav",
    "form",
    "figure",
    "figcaption",
    "dl",
    "dt",
    "dd",
    "blockquote",
    "table",
    "ul",
    "ol",
    "tr",
];

/// Elements that never take a closing tag.
const VOID: &[&str] = &[
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source",
    "track", "wbr",
];

/// Convert one HTML body to bounded markdown text.
/// @param html - Decoded HTML source, already capped by the caller.
/// @returns Markdown text for the model.
#[must_use]
pub fn html_to_markdown(html: &str) -> String {
    let sink = Sink {
        state: RefCell::new(State::default()),
    };
    let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
    let input = BufferQueue::default();
    input.push_back(StrTendril::from(html));
    let _ = tokenizer.feed(&input);
    tokenizer.end();
    let state = tokenizer.sink.state.into_inner();
    state.finish()
}

struct Sink {
    state: RefCell<State>,
}

impl TokenSink for Sink {
    type Handle = ();

    fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
        self.state.borrow_mut().token(token);
        TokenSinkResult::Continue
    }
}

#[derive(Default)]
struct State {
    out: String,
    dropped: Vec<String>,
    pre: Vec<String>,
    pre_depth: usize,
    pre_buf: String,
    lists: Vec<(bool, u64)>,
    links: Vec<Option<String>>,
    table_row_start: bool,
}

impl State {
    fn token(&mut self, token: Token) {
        match token {
            Token::CharacterTokens(text) => self.text(&text),
            Token::TagToken(tag) => self.tag(tag),
            Token::NullCharacterToken => {}
            _ => {}
        }
    }

    fn suppressed(&self) -> bool {
        !self.dropped.is_empty()
    }

    fn text(&mut self, text: &str) {
        if self.suppressed() {
            return;
        }
        if self.pre_depth > 0 {
            self.pre_buf.push_str(text);
            return;
        }
        for character in text.chars() {
            if character.is_whitespace() {
                if !self.out.is_empty() && !self.out.ends_with(' ') && !self.out.ends_with('\n') {
                    self.out.push(' ');
                }
            } else {
                self.out.push(character);
            }
        }
    }

    fn tag(&mut self, tag: Tag) {
        let name = tag.name.as_ref().to_ascii_lowercase();
        if tag.kind == TagKind::EndTag {
            self.end_tag(&name);
            return;
        }
        if self.suppressed() || hidden(&tag) || DROPPED.contains(&name.as_str()) {
            if !tag.self_closing && !VOID.contains(&name.as_str()) {
                self.dropped.push(name);
            }
            return;
        }
        if self.pre_depth > 0 {
            if name == "br" {
                self.pre_buf.push('\n');
            }
            return;
        }
        self.start_tag(&name, &tag);
    }

    fn start_tag(&mut self, name: &str, tag: &Tag) {
        if BLOCK.contains(&name) {
            self.block_break();
        }
        match name {
            "br" => self.out.push('\n'),
            "hr" => {
                self.block_break();
                self.out.push_str("---");
                self.block_break();
            }
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                self.block_break();
                let level = name[1..].parse::<usize>().unwrap_or(1);
                self.out.push_str(&"#".repeat(level));
                self.out.push(' ');
            }
            "li" => {
                self.block_break();
                match self.lists.last_mut() {
                    Some((true, count)) => {
                        *count += 1;
                        let marker = format!("{count}. ");
                        self.out.push_str(&marker);
                    }
                    _ => self.out.push_str("- "),
                }
            }
            "ul" => self.lists.push((false, 0)),
            "ol" => self.lists.push((true, 0)),
            "pre" => {
                self.block_break();
                self.pre_depth = 1;
                self.pre_buf.clear();
            }
            "code" => self.out.push('`'),
            "strong" | "b" => self.out.push_str("**"),
            "em" | "i" => self.out.push('*'),
            "a" => {
                self.links.push(attribute(tag, "href"));
                self.out.push('[');
            }
            "td" | "th" => {
                if !self.table_row_start {
                    self.trim_trailing_spaces();
                    self.out.push_str(" | ");
                }
                self.table_row_start = false;
            }
            "tr" => self.table_row_start = true,
            _ => {}
        }
    }

    fn end_tag(&mut self, name: &str) {
        if let Some(position) = self.dropped.iter().rposition(|open| open == name) {
            self.dropped.truncate(position);
            return;
        }
        if self.pre_depth > 0 {
            if name == "pre" {
                self.pre_depth = 0;
                let text = std::mem::take(&mut self.pre_buf);
                let text = text.trim_matches('\n');
                let index = self.pre.len();
                self.pre.push(format!("```\n{text}\n```"));
                self.out.push('\u{0}');
                self.out.push_str(&index.to_string());
                self.out.push('\u{0}');
                self.block_break();
            }
            return;
        }
        match name {
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "p" | "blockquote" | "dt" | "dd"
            | "figcaption" | "div" | "section" | "article" | "header" | "footer" | "main"
            | "aside" | "nav" | "form" | "figure" | "dl" | "table" => self.block_break(),
            "tr" => {
                self.out.push('\n');
                self.table_row_start = true;
            }
            "ul" | "ol" => {
                self.lists.pop();
                self.block_break();
            }
            "li" => self.block_break(),
            "code" => self.out.push('`'),
            "strong" | "b" => self.out.push_str("**"),
            "em" | "i" => self.out.push('*'),
            "a" => {
                let href = self.links.pop().flatten();
                match href {
                    Some(href) => {
                        self.out.push_str("](");
                        self.out.push_str(&href);
                        self.out.push(')');
                    }
                    None => self.out.push(']'),
                }
            }
            _ => {}
        }
    }

    fn trim_trailing_spaces(&mut self) {
        while self.out.ends_with(' ') {
            self.out.pop();
        }
    }

    fn block_break(&mut self) {
        self.trim_trailing_spaces();
        while self.out.ends_with('\n') {
            self.out.pop();
        }
        if !self.out.is_empty() {
            self.out.push_str("\n\n");
        }
    }

    fn finish(self) -> String {
        let mut text = normalize(&self.out);
        for (index, block) in self.pre.iter().enumerate() {
            let marker = format!("\u{0}{index}\u{0}");
            text = text.replace(&marker, block);
        }
        text
    }
}

/// Whether a tag hides its subtree from readers.
fn hidden(tag: &Tag) -> bool {
    if attribute(tag, "hidden").is_some() {
        return true;
    }
    if attribute(tag, "aria-hidden").is_some_and(|value| value.eq_ignore_ascii_case("true")) {
        return true;
    }
    if tag.name.as_ref().eq_ignore_ascii_case("input")
        && attribute(tag, "type").is_some_and(|value| value.eq_ignore_ascii_case("hidden"))
    {
        return true;
    }
    attribute(tag, "style").is_some_and(|style| {
        style.split(';').any(|declaration| {
            let Some((property, value)) = declaration.split_once(':') else {
                return false;
            };
            let property = property.trim().to_ascii_lowercase();
            let value = value.trim().to_ascii_lowercase();
            let value = value.trim_end_matches("!important").trim();
            (property == "display" && value == "none")
                || (property == "visibility" && (value == "hidden" || value == "collapse"))
        })
    })
}

/// Read one attribute value.
fn attribute(tag: &Tag, name: &str) -> Option<String> {
    tag.attrs
        .iter()
        .find(|attribute| attribute.name.local.as_ref().eq_ignore_ascii_case(name))
        .map(|attribute| attribute.value.to_string())
}

/// Collapse whitespace runs, drop trailing spaces, and keep at most one blank line.
fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    let mut newlines = 0usize;
    for character in text.chars() {
        match character {
            '\u{0}' => {
                if pending_space && !out.is_empty() && !out.ends_with(' ') && !out.ends_with('\n') {
                    out.push(' ');
                }
                pending_space = false;
                newlines = 0;
                out.push(character);
            }
            '\n' => {
                pending_space = false;
                newlines += 1;
            }
            character if character.is_whitespace() => pending_space = true,
            other => {
                if newlines > 0 {
                    if !out.is_empty() {
                        while out.ends_with(' ') {
                            out.pop();
                        }
                        out.push_str(if newlines > 1 { "\n\n" } else { "\n" });
                    }
                    newlines = 0;
                    pending_space = false;
                } else if pending_space {
                    if !out.is_empty() && !out.ends_with('\n') && !out.ends_with(' ') {
                        out.push(' ');
                    }
                    pending_space = false;
                }
                out.push(other);
            }
        }
    }
    while out.ends_with(['\n', ' ']) {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paragraphs_headings_and_links_become_markdown() {
        let html = "<html><head><title>t</title></head><body><h1>Title</h1><p>Hello <a href=\"https://example.com\">world</a> &amp; more.</p></body></html>";
        let text = html_to_markdown(html);
        assert!(text.starts_with("# Title"), "{text}");
        assert!(
            text.contains("[world](https://example.com) & more."),
            "{text}"
        );
        assert!(!text.contains("<p>"));
    }

    #[test]
    fn script_style_and_hidden_content_are_dropped() {
        let html = "<div>keep <script>alert(1)</script><style>.a{}</style><span hidden>secret</span><span>visible</span></div>";
        let text = html_to_markdown(html);
        assert_eq!(text, "keep visible");
    }

    #[test]
    fn lists_and_code_blocks_keep_their_structure() {
        let html = "<ul><li>one</li><li>two</li></ul><ol><li>first</li></ol><pre><code>let a = 1;\nlet b = 2;</code></pre>";
        let text = html_to_markdown(html);
        assert!(text.contains("- one"), "{text}");
        assert!(text.contains("- two"), "{text}");
        assert!(text.contains("1. first"), "{text}");
        assert!(text.contains("let a = 1;\nlet b = 2;"), "{text}");
    }

    #[test]
    fn table_cells_are_separated_on_one_row() {
        let html = "<table><tr><th>a</th><th>b</th></tr><tr><td>1</td><td>2</td></tr></table>";
        let text = html_to_markdown(html);
        assert!(text.contains("a | b"), "{text}");
        assert!(text.contains("1 | 2"), "{text}");
    }

    #[test]
    fn entities_are_decoded_and_whitespace_collapsed() {
        let html = "<p>a&nbsp;&nbsp;  b\n\n c &lt;tag&gt;</p>";
        let text = html_to_markdown(html);
        assert_eq!(text, "a b c <tag>");
    }
}
