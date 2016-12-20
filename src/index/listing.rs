use utils::ring_buffer::RingBuffer;

use compressor::{Compressor, NaiveCompressor};

use page_manager::{PageId, Block, BlockId, RamPageCache, BlockManager};

use index::posting::Posting;

type UsedCompressor = NaiveCompressor;

pub struct Listing {
    block_list: Vec<PageId>,
    last_block_id: BlockId,
    posting_buffer: RingBuffer<Posting>,
}

impl Listing {

    pub fn new() -> Self  {
        Listing {
            block_list: Vec::new(),
            last_block_id: BlockId::last(),
            posting_buffer: RingBuffer::new()
        }

    }
    
    pub fn add(&mut self, postings: &[Posting], page_cache: &mut RamPageCache) {
        for posting in postings {
            self.posting_buffer.push_back(*posting);
        }
        self.compress_and_ship(page_cache, false);
    }

    pub fn commit(&mut self, page_cache: &mut RamPageCache) {
        self.compress_and_ship(page_cache, true);
        page_cache.flush_page(*self.block_list.last().unwrap());
    }

    fn compress_and_ship(&mut self, page_cache: &mut RamPageCache, force: bool) {
        while let Some(block) = UsedCompressor::compress(&mut self.posting_buffer) {
            self.ship(page_cache, block);
        }
        if force && self.posting_buffer.count() > 0 {
            let block = UsedCompressor::force_compress(&mut self.posting_buffer);
            self.ship(page_cache, block);
        }
    }

    fn ship(&mut self, page_cache: &mut RamPageCache, block: Block) {
        if self.last_block_id == BlockId::last() {
            self.block_list.push(page_cache.store_block(block));
            self.last_block_id = BlockId::first();
        } else {
            self.last_block_id.inc();
            page_cache.store_in_place(*self.block_list.last().unwrap(), self.last_block_id, block)
        }
    }
}


#[cfg(test)]
mod tests {

    use super::Listing;
    
    use std::sync::Arc;
    use test_utils::create_test_dir;

    use index::posting::{Posting, DocId};
    use page_manager::{BlockManager, FsPageManager, Page, RamPageCache, PageId, Block, BlockId,
                       BLOCKSIZE};


    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("listing/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }
    
    #[test]
    fn basic_add() {
        let mut cache = new_cache("basic_add");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.block_list.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
    }
    
    #[test]
    fn commit() {
        let mut cache = new_cache("commit");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.block_list.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
        listing.commit(&mut cache);
        assert_eq!(listing.block_list.len(), 1);
        assert_eq!(listing.posting_buffer.count(), 0);
        assert_eq!(listing.last_block_id, BlockId::first());
    }


    #[test]
    fn add() {
        let mut cache = new_cache("add");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.block_list.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
        for i in 0..100 {
            listing.add(&[Posting(DocId(i))], &mut cache);
        }
        assert!(listing.block_list.len() > 0);
        assert!(listing.posting_buffer.count() > 0);
        listing.commit(&mut cache);
        assert_eq!(listing.posting_buffer.count(), 0);
    }

    #[test]
    fn add_much() {
        let mut cache = new_cache("add_much");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.block_list.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
        for i in 0..10000 {
            listing.add(&[Posting(DocId(i))], &mut cache);
        }
        assert!(listing.block_list.len() > 0);
        assert!(listing.posting_buffer.count() > 0);
        listing.commit(&mut cache);
        assert_eq!(listing.posting_buffer.count(), 0);
    }

    #[test]
    fn multiple_listings() {
        let mut cache = new_cache("multiple_listings");
        let mut listings = (0..100).map(|_| Listing::new()).collect::<Vec<_>>();
        for i in 0..50000 {
            listings[i%100].add(&[Posting(DocId(i as u64))], &mut cache);
        }
        for listing in listings.iter_mut() {
            assert!(listing.block_list.len() > 0);
            assert!(listing.posting_buffer.count() > 0);
            listing.commit(&mut cache);
        }
        for listing in listings {
            assert_eq!(listing.posting_buffer.count(), 0);
        }
    }
}