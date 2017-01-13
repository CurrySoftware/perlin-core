//! perlin-core is a low level information-retrieval library
//!
//! It does not know about strings, language or documents.
//! If you are looking for a fully featured document search engine please refer
//! to [https://github.com/JDemler/perlin]
//!
//! Here you will find the basic building blocks on which perlin is build upon!
#[macro_use]
mod utils;
mod compressor;
pub mod page_manager;
pub mod index;

#[cfg(test)]
pub mod test_utils;
