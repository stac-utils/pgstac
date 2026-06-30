use crate::Error;
use crate::search::SearchPage;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac::Link;
use stac::api::{Context, Item};

/// A page of search results.
#[derive(Debug, Deserialize, Serialize)]
pub struct Page {
    /// These are the out features, usually STAC items, but maybe not legal STAC
    /// items if fields are excluded.
    pub features: Vec<Item>,

    /// The next id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,

    /// The previous id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,

    /// The search context.
    ///
    /// This was removed in pgstac v0.9
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// The number of values returned.
    ///
    /// Added in pgstac v0.9
    #[serde(rename = "numberReturned", skip_serializing_if = "Option::is_none")]
    pub number_returned: Option<usize>,

    /// Links
    ///
    /// Added in pgstac v0.9
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

impl Page {
    /// Returns this page's next token, if it has one.
    pub fn next_token(&self) -> Option<String> {
        if let Some(next) = &self.next {
            return Some(format!("next:{next}"));
        }

        self.links
            .iter()
            .find(|link| link.rel == "next")
            .and_then(|link| extract_token_from_href(&link.href))
    }

    /// Returns this page's prev token, if it has one.
    pub fn prev_token(&self) -> Option<String> {
        if let Some(prev) = &self.prev {
            return Some(format!("prev:{prev}"));
        }

        self.links
            .iter()
            .find(|link| link.rel == "prev")
            .and_then(|link| extract_token_from_href(&link.href))
    }
}

impl TryFrom<SearchPage> for Page {
    type Error = Error;

    /// Adapts the Rust engine's [`SearchPage`] into the rustac [`Page`] shape: deserialize the hydrated
    /// feature values into [`Item`]s, carry the keyset tokens (the engine prefixes them `next:`/`prev:`,
    /// which [`Page::next_token`]/[`Page::prev_token`] re-add), and surface the match count as a
    /// [`Context`] when the search counted one.
    fn try_from(page: SearchPage) -> Result<Self, Error> {
        let features = page
            .features
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<Item>, _>>()?;
        let strip = |token: Option<String>, prefix: &str| {
            token.and_then(|t| t.strip_prefix(prefix).map(str::to_string))
        };
        let context = page.number_matched.map(|matched| Context {
            returned: page.number_returned as u64,
            limit: None,
            matched: Some(matched as u64),
            additional_fields: Map::new(),
        });
        Ok(Page {
            features,
            next: strip(page.next_token, "next:"),
            prev: strip(page.prev_token, "prev:"),
            context,
            number_returned: Some(page.number_returned),
            links: Vec::new(),
            additional_fields: Map::new(),
        })
    }
}

fn extract_token_from_href(href: &str) -> Option<String> {
    href.split("token=")
        .nth(1)
        .and_then(|token_part| token_part.split('&').next())
        .map(|token| token.to_string())
}
