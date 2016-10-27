//! This module provides the implementation for boolean information retrieval
//! Use `IndexBuilder` to build indices
//! Use `QueryBuilder` to build queries that run on these indices
use std;
use std::io;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::iter::Iterator;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::thread;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::mpsc;

use index::Index;
use storage::{Storage, StorageError};
use storage::chunked_storage::{IndexingChunk, ChunkedStorage};
use index::boolean_index::boolean_query::*;
use index::boolean_index::query_result_iterator::*;
use index::boolean_index::query_result_iterator::nary_query_iterator::*;
use index::boolean_index::posting::{decode_from_chunk, Listing};

use storage::compression::{vbyte_encode, VByteDecoder};
use storage::{ByteEncodable, ByteDecodable, DecodeError};
use utils::owning_iterator::ArcIter;
use utils::persistence::Persistent;

pub use index::boolean_index::query_builder::QueryBuilder;
pub use index::boolean_index::index_builder::IndexBuilder;

mod query_result_iterator;
mod index_builder;
mod query_builder;
mod posting;
mod boolean_query;

const VOCAB_FILENAME: &'static str = "vocabulary.bin";
const STATISTICS_FILENAME: &'static str = "statistics.bin";
const CHUNKSIZE: usize = 1_000_000;
const SORT_THREADS: usize = 4;

/// A specialized `Result` type for operations related to `BooleanIndex`
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
/// Error kinds that can occure during indexing operations
pub enum IndexingError {
    /// An Error related to sending via `mpsc::Channel`
    Send,
    /// An indexing thread panicked
    ThreadPanic,
}

#[derive(Debug)]
/// Error kinds that can occur during operations related to `BooleanIndex`
pub enum Error {
    /// A persistent `BooleanIndex` should be build but no path where to persist it was specified
    /// Call the `IndexBuilder::persist()`
    PersistPathNotSpecified,
    /// A `BooleanIndex` should be loaded from a directory but the specified directory is empty
    MissingIndexFiles(Vec<&'static str>),
    /// A `BooleanIndex` attempted to beeing loaded from a file, not a directory
    PersistPathIsFile,
    /// Tried to load a `BooleanIndex` from a corrupted file
    CorruptedIndexFile(Option<DecodeError>),
    /// An IO-Error occured
    IO(io::Error),
    /// A Storage-Error occured
    Storage(StorageError),
    /// An error occured during indexing
    Indexing(IndexingError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        Error::Storage(err)
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(_: mpsc::SendError<T>) -> Self {
        Error::Indexing(IndexingError::Send)
    }
}

/// Implements the `Index` trait. Limited to boolean retrieval.
pub struct BooleanIndex<TTerm: Ord + Hash> {
    document_count: usize,
    term_ids: HashMap<TTerm, u64>,
    chunked_postings: ChunkedStorage,
    persist_path: Option<PathBuf>,
}

// Index implementation
impl<'a, TTerm: Ord + Hash> Index<'a, TTerm> for BooleanIndex<TTerm> {
    type Query = BooleanQuery<TTerm>;
    type QueryResult = Box<Iterator<Item = u64>>;

    /// Executes a `BooleanQuery` and returns a boxed iterator over the resulting document ids.
    /// The query execution is lazy.
    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult {
        Box::new(self.run_query(query))
    }
}


fn decode_postings(storage: &ChunkedStorage, id: u64) -> Option<Listing> {
    // Get hot listing
    let chunk = storage.get_current(id);
    let mut listing = decode_from_chunk(&mut chunk.get_bytes()).unwrap();
    let mut previous = chunk.previous_chunk();
    // If there are predecessors, get them, decode them and append them to the result.
    // Currently not very efficient.
    // TODO: Turn that into threaded lazy iterators
    while previous.is_some() {
        let chunk = storage.get_archived(previous.unwrap());
        previous = chunk.previous_chunk();
        match decode_from_chunk(&mut chunk.get_bytes()) {
            Ok(mut new) => {
                new.append(&mut listing);
                listing = new;
            }
            //TODO: Errorhandling
            Err((doc_id, position)) => {
                println!("{}-{}", doc_id, position);
                println!("{:?}", chunk);
                panic!("TF");
            }
        }
    }
    return Some(listing);
}

impl<TTerm> BooleanIndex<TTerm>
    where TTerm: Ord + ByteDecodable + ByteEncodable + Hash
{
    /// Load a `BooleanIndex` from a previously populated folder
    /// Not intended for public use. Please use the `IndexBuilder` instead
    fn load<TStorage>(path: &Path) -> Result<Self>
        where TStorage: Storage<IndexingChunk> + Persistent + 'static
    {
        let storage = try!(TStorage::load(path));
        let vocab = try!(Self::load_vocabulary(path));
        let doc_count = try!(Self::load_statistics(path));
        let chunked_storage = ChunkedStorage::load(path, Box::new(storage)).unwrap();
        BooleanIndex::from_parts(chunked_storage, vocab, doc_count)
    }

    /// Creates a new `BooleanIndex` instance which is written to the passed
    /// path
    /// Not intended for public use. Please use the `IndexBuilder` instead
    fn new_persistent<TDocsIterator, TDocIterator, TStorage>(storage: TStorage,
                                                             documents: TDocsIterator,
                                                             path: &Path)
                                                             -> Result<Self>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<IndexingChunk> + Persistent + 'static
    {
        let (document_count, chunked_postings, term_ids) = try!(BooleanIndex::index_documents(documents, storage));
        let index = BooleanIndex {
            document_count: document_count,
            term_ids: term_ids,
            persist_path: Some(path.to_path_buf()),
            // Initialized by index_documents
            chunked_postings: chunked_postings,
        };
        try!(index.save_vocabulary());
        try!(index.save_statistics());
        try!(index.chunked_postings.persist(path));
        Ok(index)
    }

    fn save_vocabulary(&self) -> Result<()> {
        if let Some(filename) = self.persist_path.as_ref().map(|p| p.join(VOCAB_FILENAME)) {
            // Open file
            let mut vocab_file = try!(OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(filename));
            // Iterate over vocabulary and encode data
            let mut byte_buffer = Vec::with_capacity(2 * CHUNKSIZE);
            for vocab_entry in &self.term_ids {
                // Encode term and number of its bytes
                let term_bytes = vocab_entry.0.encode();
                let term_length_bytes = vbyte_encode(term_bytes.len());
                // Encode id
                let id_bytes = vbyte_encode(*vocab_entry.1 as usize);

                // Append id, term length and term to byte_buffer
                byte_buffer.extend_from_slice(&id_bytes);
                byte_buffer.extend_from_slice(&term_length_bytes);
                byte_buffer.extend_from_slice(&term_bytes);

                // Write if buffer is full
                if byte_buffer.len() > CHUNKSIZE {
                    try!(vocab_file.write(&byte_buffer));
                    byte_buffer.clear();
                }
            }
            if !byte_buffer.is_empty() {
                // If some rests are in the buffer write them to file
                try!(vocab_file.write(&byte_buffer));
            }
            Ok(())
        } else {
            Err(Error::PersistPathNotSpecified)
        }
    }

    fn load_vocabulary(path: &Path) -> Result<HashMap<TTerm, u64>> {
        // Open file
        let vocab_file = try!(OpenOptions::new().read(true).open(path.join(VOCAB_FILENAME)));
        // Create a decoder from that vector
        let mut decoder = VByteDecoder::new(vocab_file.bytes());
        let mut result = HashMap::new();
        while let Some(id) = decoder.next() {
            if let Some(term_len) = decoder.next() {
                let term_bytes: Vec<u8> = decoder.underlying_iterator().take(term_len).map(|b| b.unwrap()).collect();
                match TTerm::decode(&mut term_bytes.as_slice()) {
                    Ok(term) => result.insert(term, id as u64),
                    Err(e) => return Err(Error::CorruptedIndexFile(Some(e))),
                };
            } else {
                return Err(Error::CorruptedIndexFile(None));
            }
        }
        Ok(result)
    }

    fn save_statistics(&self) -> Result<()> {
        // Open file
        if let Some(filename) = self.persist_path.as_ref().map(|p| p.join(STATISTICS_FILENAME)) {
            let mut statistics_file = try!(OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(filename));
            try!(statistics_file.write(&vbyte_encode(self.document_count)));
            Ok(())
        } else {
            Err(Error::PersistPathNotSpecified)
        }
    }

    fn load_statistics(path: &Path) -> Result<usize> {
        let statistics_file = try!(OpenOptions::new().read(true).open(path.join(STATISTICS_FILENAME)));
        if let Some(doc_count) = VByteDecoder::new(statistics_file.bytes()).next() {
            Ok(doc_count)
        } else {
            Err(Error::CorruptedIndexFile(None))
        }
    }
}

impl<TTerm: Ord + Hash> BooleanIndex<TTerm> {
    /// Returns the number of indexed documents
    pub fn document_count(&self) -> usize {
        self.document_count
    }

    /// Creates a new volatile `BooleanIndex`. Not intended for public use.
    /// Please use `IndexBuilder` instead
    fn new<TDocsIterator, TDocIterator, TStorage>(storage: TStorage, documents: TDocsIterator) -> Result<Self>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<IndexingChunk> + 'static
    {
        let (document_count, chunked_postings, term_ids) = try!(BooleanIndex::index_documents(documents, storage));
        let index = BooleanIndex {
            document_count: document_count,
            term_ids: term_ids,
            persist_path: None,
            chunked_postings: chunked_postings,
        };
        Ok(index)
    }




    fn from_parts(inverted_index: ChunkedStorage,
                  vocabulary: HashMap<TTerm, u64>,
                  document_count: usize)
                  -> Result<Self> {
        Ok(BooleanIndex {
            document_count: document_count,
            term_ids: vocabulary,
            chunked_postings: inverted_index,
            persist_path: None,
        })
    }

    /// Indexes a document collection for later retrieval
    /// Returns the number of documents indexed
    fn index_documents<TDocsIterator, TDocIterator, TStorage>(documents: TDocsIterator,
                                                              storage: TStorage)
                                                              -> Result<(usize, ChunkedStorage, HashMap<TTerm, u64>)>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<IndexingChunk> + 'static
    {
        let (merged_tx, merged_rx) = mpsc::sync_channel(64);
        let mut document_count = 0;
        // Initialize and start sorting threads
        let mut chunk_tx = Vec::with_capacity(SORT_THREADS);
        let mut sort_threads = Vec::with_capacity(SORT_THREADS);
        for _ in 0..SORT_THREADS {
            let (tx, rx) = mpsc::channel();
            chunk_tx.push(tx);
            let m_tx = merged_tx.clone();
            sort_threads.push(thread::spawn(|| BooleanIndex::<TTerm>::sort_and_group_chunk(rx, m_tx)));
        }
        drop(merged_tx);
        let inv_index = thread::spawn(|| BooleanIndex::<TTerm>::invert_index(merged_rx, storage));
        let mut term_ids = HashMap::new();
        let mut buffer = Vec::with_capacity(213400);
        let mut term_count = 0;
        // For every document in the collection
        let mut chunk_count = 0;
        for (doc_id, document) in documents.enumerate() {
            // Enumerate over its terms
            for (term_position, term) in document.into_iter().enumerate() {
                // Has term already been seen? Is it already in the vocabulary?
                if let Some(term_id) = term_ids.get(&term) {
                    buffer.push((*term_id, doc_id as u64, term_position as u32));
                    continue;
                }
                term_ids.insert(term, term_count as u64);
                buffer.push((term_count as u64, doc_id as u64, term_position as u32));
                term_count += 1;
            }
            // Term was not yet indexed. Add it
            document_count += 1;
            if document_count % 256 == 0 {
                let index = chunk_count % SORT_THREADS;
                let old_len = buffer.len();
                try!(chunk_tx[index].send(buffer));
                buffer = Vec::with_capacity(old_len + old_len / 10);
                chunk_count += 1;
            }
        }
        try!(chunk_tx[chunk_count % SORT_THREADS].send(buffer));
        drop(chunk_tx);
        // Join sort threads
        if sort_threads.into_iter().any(|thread| thread.join().is_err()) {
            return Err(Error::Indexing(IndexingError::ThreadPanic));
        }
        // Join invert index thread and save result
        let chunked_postings = match inv_index.join() {
            Ok(res) => try!(res),
            Err(_) => return Err(Error::Indexing(IndexingError::ThreadPanic)),
        };

        Ok((document_count, chunked_postings, term_ids))
    }

    fn sort_and_group_chunk(ids: mpsc::Receiver<Vec<(u64, u64, u32)>>,
                            grouped_chunks: mpsc::SyncSender<Vec<(u64, Listing)>>) {

        while let Ok(mut chunk) = ids.recv() {
            // Sort triples by term_id
            chunk.sort_by_key(|&(a, _, _)| a);
            let mut grouped_chunk = Vec::with_capacity(chunk.len());
            let mut last_tid = 0;
            let mut term_counter = 0;
            // Group by term_id and doc_id
            for (i, &(term_id, doc_id, pos)) in chunk.iter().enumerate() {
                // if term is the first term or different to the last term (new group)
                if last_tid < term_id || i == 0 {
                    term_counter += 1;
                    // Term_id has to be added
                    grouped_chunk.push((term_id, vec![(doc_id, vec![pos])]));
                    last_tid = term_id;
                    continue;
                }
                // Term_id is already known.
                {
                    let mut posting = grouped_chunk[term_counter - 1].1.last_mut().unwrap();
                    // Check if last doc_id equals this doc_id
                    if posting.0 == doc_id {
                        // If so only push the new position
                        posting.1.push(pos);
                        continue;
                    }
                }
                // Otherwise add a whole new posting
                grouped_chunk[term_counter - 1].1.push((doc_id, vec![pos]));
            }
            // Send grouped chunk to merger thread
            // (yes, this is a verb: https://en.wiktionary.org/wiki/grouped#English)
            grouped_chunks.send(grouped_chunk).unwrap();
        }
    }

    fn invert_index<TStorage>(grouped_chunks: mpsc::Receiver<Vec<(u64, Listing)>>,
                              storage: TStorage)
                              -> Result<ChunkedStorage>
        where TStorage: Storage<IndexingChunk> + 'static
    {
        let mut storage = ChunkedStorage::new(10000, Box::new(storage));
        while let Ok(chunk) = grouped_chunks.recv() {
            let threshold = storage.len();
            for (term_id, listing) in chunk {
                let uterm_id = term_id as usize;
                // Get chunk to write to or create if unknown
                let result = {
                    let stor_chunk = if uterm_id < threshold {
                        storage.get_current_mut(term_id)
                    } else {
                        storage.new_chunk(term_id)
                    };
                    stor_chunk.append(&listing)
                };
                // Listing did not fit into current chunk completly
                // Get the next and put it in there.
                // Repeat until done
                if let Err(mut position) = result {
                    loop {
                        let next_chunk = try!(storage.next_chunk(term_id));
                        if let Err(new_position) = next_chunk.append(&listing[position..]) {
                            if new_position == 0 {
                                // TODO: FIXME
                                panic!("Position list was longer than chunksize. Go Home!");
                            }
                            position += new_position;
                        } else {
                            break;
                        }
                    }
                }
            }

        }
        Ok(storage)
    }


    fn run_query(&self, query: &BooleanQuery<TTerm>) -> QueryResultIterator {
        match *query {
            BooleanQuery::Atom(ref atom) => self.run_atom(atom.relative_position, &atom.query_term),
            BooleanQuery::NAry(ref operator, ref operands) => self.run_nary_query(operator, operands),
            BooleanQuery::Positional(ref operator, ref operands) => self.run_positional_query(operator, operands),
            BooleanQuery::Filter(ref operator, ref sand, ref sieve) => {
                self.run_filter(operator, sand.as_ref(), sieve.as_ref())
            }

        }

    }

    fn run_nary_query(&self, operator: &BooleanOperator, operands: &[BooleanQuery<TTerm>]) -> QueryResultIterator {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_query(operand))
        }
        QueryResultIterator::NAry(NAryQueryIterator::new(*operator, ops))
    }

    fn run_positional_query(&self,
                            operator: &PositionalOperator,
                            operands: &[QueryAtom<TTerm>])
                            -> QueryResultIterator {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_atom(operand.relative_position, &operand.query_term));
        }
        QueryResultIterator::NAry(NAryQueryIterator::new_positional(*operator, ops))
    }

    fn run_filter(&self,
                  operator: &FilterOperator,
                  sand: &BooleanQuery<TTerm>,
                  sieve: &BooleanQuery<TTerm>)
                  -> QueryResultIterator {
        QueryResultIterator::Filter(FilterIterator::new(*operator,
                                                        Box::new(self.run_query(sand)),
                                                        Box::new(self.run_query(sieve))))
    }


    fn run_atom(&self, relative_position: usize, atom: &TTerm) -> QueryResultIterator {
        if let Some(result) = self.term_ids.get(atom) {
            QueryResultIterator::Atom(relative_position,
                                      ArcIter::new(Arc::new(decode_postings(&self.chunked_postings, *result).unwrap())))
        } else {
            QueryResultIterator::Empty
        }
    }
}



// --- Tests

#[cfg(test)]
mod tests {

    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use index::boolean_index::boolean_query::*;

    use index::Index;
    use storage::{FsStorage, RamStorage};


    pub fn prepare_index() -> BooleanIndex<usize> {
        let index = IndexBuilder::<_, RamStorage<_>>::new().create(vec![(0..10).collect::<Vec<_>>().into_iter(),
                                                                        (0..10)
                                                                            .map(|i| i * 2)
                                                                            .collect::<Vec<_>>()
                                                                            .into_iter(),
                                                                        vec![5, 4, 3, 2, 1, 0].into_iter()]
            .into_iter());
        index.unwrap()
    }

    #[test]
    fn empty_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 15)))
            .collect::<Vec<_>>() == vec![]);

    }



    #[test]
    fn indexing() {
        let index = prepare_index();
        // Check number of docs
        assert!(index.document_count == 3);
        // Check number of terms (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18)
        assert!(index.term_ids.len() == 15);
        // assert!(*index.postings.get(*index.term_ids.get(&0).unwrap()).unwrap() ==
        //         vec![(0, vec![0]), (1, vec![0]), (2, vec![5])]);
        assert_eq!(index.document_count(), 3);

    }

    #[test]
    fn query_atom() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
            .collect::<Vec<_>>() == vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
            .collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn nary_query() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 5)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 0))]))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 5))]))
            .collect::<Vec<_>>() == vec![0, 2]);
    }

    #[test]
    fn and_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 12))]))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 12))]))
            .collect::<Vec<_>>() == vec![1]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::NAry(BooleanOperator::And,
                                                                       vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 12))]))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::NAry(BooleanOperator::And,
                                                                       vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                        BooleanQuery::Atom(QueryAtom::new(0, 4))]),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
            .collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn or_query() {
        let index = prepare_index();
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                               BooleanQuery::Atom(QueryAtom::new(0, 12))]))
                       .collect::<Vec<_>>(),
                   vec![0, 1, 2]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                               BooleanQuery::Atom(QueryAtom::new(0, 12))]))
                       .collect::<Vec<_>>(),
                   vec![1]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::NAry(BooleanOperator::Or,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
                       .collect::<Vec<_>>(),
                   vec![0, 1, 2]);
    }

    #[test]
    fn inorder_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0), QueryAtom::new(1, 1)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(1, 0), QueryAtom::new(0, 1)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0), QueryAtom::new(1, 2)]))
            .collect::<Vec<_>>() == vec![1]);

        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(2, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(0, 0)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(2, 0)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(3, 0)]))
            .collect::<Vec<_>>() == vec![]);
    }

    #[test]
    fn query_filter() {
        let index = prepare_index();
        assert!(index.execute_query(
            &BooleanQuery::Filter(FilterOperator::Not,
            Box::new(BooleanQuery::NAry(
                BooleanOperator::And,
                vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                     BooleanQuery::Atom(QueryAtom::new(0, 0))])),
                      Box::new(BooleanQuery::Atom(
                          QueryAtom::new(0, 16))))).collect::<Vec<_>>() == vec![0,2]);
    }


    #[test]
    fn persistence() {
        assert!(create_dir_all(Path::new("/tmp/persistent_index_test")).is_ok());
        {
            let index = IndexBuilder::<u32, FsStorage<_>>::new()
                .persist(Path::new("/tmp/persistent_index_test"))
                .create_persistent(vec![(0..10).collect::<Vec<_>>().into_iter(),
                                        (0..10).map(|i| i * 2).collect::<Vec<_>>().into_iter(),
                                        vec![5, 4, 3, 2, 1, 0].into_iter()]
                    .into_iter())
                .unwrap();

            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
                .collect::<Vec<_>>() == vec![0, 1, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
                .collect::<Vec<_>>() == vec![1]);
        }

        {
            let index = IndexBuilder::<usize, FsStorage<_>>::new()
                .persist(Path::new("/tmp/persistent_index_test"))
                .load()
                .unwrap();
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
                .collect::<Vec<_>>() == vec![0, 1, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
                .collect::<Vec<_>>() == vec![1]);
        }
    }
}
