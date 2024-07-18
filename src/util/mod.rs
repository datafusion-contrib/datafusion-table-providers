use datafusion::logical_expr::Expr;
use snafu::prelude::*;
use std::collections::HashMap;
use std::hash::Hash;

use crate::sql::sql_provider_datafusion::expr::{self, Engine};

pub mod column_reference;
pub mod constraints;
pub mod indexes;
pub mod ns_lookup;
pub mod on_conflict;
pub mod retriable_error;
pub mod secrets;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },
}

pub fn filters_to_sql(filters: &[Expr], engine: Option<Engine>) -> Result<String, Error> {
    Ok(filters
        .iter()
        .map(|expr| expr::to_sql_with_engine(expr, engine))
        .collect::<expr::Result<Vec<_>>>()
        .context(UnableToGenerateSQLSnafu)?
        .join(" AND "))
}

#[must_use]
pub fn hashmap_from_option_string<K, V>(hashmap_option_str: &str) -> HashMap<K, V>
where
    K: for<'a> From<&'a str> + Eq + Hash,
    V: for<'a> From<&'a str> + Default,
{
    hashmap_option_str
        .split(';')
        .map(|index| {
            let parts: Vec<&str> = index.split(':').collect();
            if parts.len() == 2 {
                (K::from(parts[0]), V::from(parts[1]))
            } else {
                (K::from(index), V::default())
            }
        })
        .collect()
}

#[must_use]
pub fn remove_prefix_from_hashmap_keys<V>(
    hashmap: HashMap<String, V>,
    prefix: &str,
) -> HashMap<String, V> {
    hashmap
        .into_iter()
        .map(|(key, value)| {
            let new_key = key
                .strip_prefix(prefix)
                .map_or(key.clone(), |s| s.to_string());
            (new_key, value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_remove_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("prefix_key1".to_string(), "value1".to_string());
        hashmap.insert("prefix_key2".to_string(), "value2".to_string());
        hashmap.insert("key3".to_string(), "value3".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        expected.insert("key3".to_string(), "value3".to_string());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_no_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("key1".to_string(), "value1".to_string());
        hashmap.insert("key2".to_string(), "value2".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_hashmap() {
        let hashmap: HashMap<String, String> = HashMap::new();

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let expected: HashMap<String, String> = HashMap::new();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_full_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("prefix_".to_string(), "value1".to_string());
        hashmap.insert("prefix_key2".to_string(), "value2".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let mut expected = HashMap::new();
        expected.insert("".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());

        assert_eq!(result, expected);
    }
}
