//! This module provides the trait `SeekingIterator`.
//!
//! A `SeekingIterator`, as the name suggests, can seek to a certain position.
//!

use std::option::Option;

/// Trait that defines an iterator type that allow seeking access.
/// This is especially usefull for query evaluation.
pub trait SeekingIterator {
    type Item;

    /// Yields an Item that is >= the passed argument or None if no such element exists
    fn next_seek(&mut self, &Self::Item) -> Option<Self::Item>;
}
