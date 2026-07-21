//! Adapts the keyset engine's [`SearchPage`] into the rustac-native [`stac::api::ItemCollection`].

use crate::Error;
use crate::search::SearchPage;
use serde_json::{Map, Value};
use stac::api::{Context, Item, ItemCollection};

/// Adapts the engine's [`SearchPage`] into a [`stac::api::ItemCollection`]: the hydrated feature values
/// parse into [`Item`]s, the `next:`/`prev:`-prefixed keyset tokens become `{"token": …}` pagination
/// maps, and the match count (when the search counted one) surfaces as both `number_matched` and a
/// [`Context`]. This is the single page-to-ItemCollection path the `stac::api` client-trait impls route
/// through.
impl TryFrom<SearchPage> for ItemCollection {
    type Error = Error;

    fn try_from(page: SearchPage) -> Result<Self, Error> {
        let items = page
            .features
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<Item>, _>>()?;
        let token_map = |token: String| {
            let mut map = Map::new();
            let _ = map.insert("token".into(), Value::String(token));
            map
        };
        let context = page.number_matched.map(|matched| Context {
            returned: page.number_returned as u64,
            limit: None,
            matched: Some(matched as u64),
            additional_fields: Map::new(),
        });
        let number_matched = context.as_ref().and_then(|context| context.matched);
        let mut item_collection = ItemCollection::new(items)?;
        item_collection.number_matched = number_matched;
        item_collection.number_returned = Some(page.number_returned as u64);
        item_collection.context = context;
        item_collection.next = page.next_token.map(token_map);
        item_collection.prev = page.prev_token.map(token_map);
        Ok(item_collection)
    }
}
