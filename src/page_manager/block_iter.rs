use std::sync::Arc;
use std::usize;

use page_manager::{Pages, Page, PageId, BlockId, Block, RamPageCache, PageCache, PAGESIZE};

#[derive(Clone, Debug)]
pub struct BlockIter<'a> {
    cache: &'a RamPageCache,
    pages: Pages,
    current_page: (PageId, Arc<Page>),
    page_index: usize,
    ptr: usize
}

impl<'a> BlockIter<'a> {
    pub fn new(cache: &'a RamPageCache, pages: Pages) -> Self {
        BlockIter {
            cache: cache,
            pages: pages,
            current_page: (PageId::none(), Arc::new(Page::empty())),
            page_index: usize::MAX,
            ptr: 0,
        }
    }

    fn get_page(&mut self) -> Option<()> {
        //On what page are we?
        let page_id = self.pages.get(self.calc_page_index())?;
        let page = self.cache.get_page(page_id);
        self.page_index = self.calc_page_index();
        self.current_page = (page_id, page);
        Some(())
    }

    fn calc_page_index(&self) -> usize {
        self.ptr/PAGESIZE
    }

    fn calc_block_index(&self) -> Option<BlockId> {
        if self.page_index == self.pages.len() -1 && self.pages.has_unfull()  {
            let res = BlockId((self.ptr % PAGESIZE) as u16 + self.pages.unfull().unwrap().from().0);
            if res.0 >= self.pages.unfull().unwrap().to().0 {
                None
            } else {
                Some(res)
            }
        } else {
            Some(BlockId((self.ptr % PAGESIZE) as u16))
        }
    }

    pub fn skip_blocks(&mut self, by: usize) {
        self.ptr += by;
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = Block;

    fn next(&mut self) -> Option<Self::Item> {
        if self.calc_page_index() != self.page_index {
            self.get_page()?;
        }
        let block_index = self.calc_block_index()?;
        self.ptr += 1;
        Some(self.current_page.1[block_index])
    }
}


#[cfg(test)]
mod tests {
    use test_utils::create_test_dir;

    use super::BlockIter;
    use page_manager::{UnfullPage, RamPageCache, BlockManager, FsPageManager, Pages, PageId, Block,
                       BlockId, BLOCKSIZE, PAGESIZE};



    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("block_iter/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }

    #[test]
    fn basic() {
        let mut cache = new_cache("basic");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([(j % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }
        let pages = Pages((0..2048).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let mut iter = BlockIter::new(&cache, pages);
        for i in 0..2048 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
    }

    #[test]
    fn unfull() {
        let mut cache = new_cache("unfull");
        assert_eq!(cache.store_block(Block([1; BLOCKSIZE])), PageId(0));
        assert_eq!(cache.flush_unfull(PageId(0), BlockId(1)),
                   UnfullPage::new(PageId(0), BlockId(1), BlockId(2)));
        let mut iter =
            BlockIter::new(&cache,
                           Pages(Vec::new(),
                                 Some(UnfullPage::new(PageId(0), BlockId(1), BlockId(2)))));
        assert_eq!(iter.next(), Some(Block([1; BLOCKSIZE])));
        assert_eq!(iter.next(), None);
    }


    #[test]
    fn full_unfull() {
        let mut cache = new_cache("full_unfull");
        // Fill with full pages
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([(j % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }
        // Add Unfull page
        assert_eq!(cache.store_block(Block([1; BLOCKSIZE])), PageId(2048));
        assert_eq!(cache.flush_unfull(PageId(2048), BlockId(1)),
                   UnfullPage::new(PageId(2048), BlockId(1), BlockId(2)));
        let mut iter =
            BlockIter::new(&cache,
                           Pages((0..2048).map(|i| PageId(i)).collect::<Vec<_>>(),
                                 Some(UnfullPage::new(PageId(2048), BlockId(1), BlockId(2)))));
        // Iterate over full pages
        for i in 0..2048 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
        // Unfull page
        assert_eq!(iter.next(), Some(Block([1; BLOCKSIZE])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn filled_unfull() {
        let mut cache = new_cache("filled_unfull");
        assert_eq!(cache.store_block(Block([0; BLOCKSIZE])), PageId(0));
        for i in 1..PAGESIZE - 1 {
            cache.store_in_place(PageId(0),
                                 BlockId(i as u16),
                                 Block([(i % 255) as u8; BLOCKSIZE]));
        }
        let unfull_page = cache.flush_unfull(PageId(0), BlockId::last());
        let mut iter = BlockIter::new(&cache, Pages(Vec::new(), Some(unfull_page)));
        for i in 0..PAGESIZE - 1 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
        }
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multiple_readers() {
        let mut cache = new_cache("multiple_readers");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([(j % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }
        let pages1 = Pages((0..1024).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let pages2 = Pages((1024..2048).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let mut iter1 = BlockIter::new(&cache, pages1);
        let mut iter2 = BlockIter::new(&cache, pages2);
        for i in 0..1024 {
            assert_eq!(iter1.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            assert_eq!(iter2.next(),
                       Some(Block([((i + 1024) % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter1.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
                assert_eq!(iter2.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
    }

    #[test]
    fn skip_blocks() {
        let mut cache = new_cache("skip_blocks");
        // Fill 2048 pages
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([(j % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }

        let pages = Pages((0..2048).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let mut iter = BlockIter::new(&cache, pages);
        assert_eq!(iter.next(), Some(Block([0; BLOCKSIZE])));
        iter.skip_blocks(15);
        assert_eq!(iter.next(), Some(Block([16; BLOCKSIZE])));
        iter.skip_blocks(63);
        assert_eq!(iter.next(), Some(Block([16; BLOCKSIZE])));
        iter.skip_blocks(128);
        assert_eq!(iter.next(), Some(Block([17; BLOCKSIZE])));
        iter.skip_blocks(1);
        assert_eq!(iter.next(), Some(Block([19; BLOCKSIZE])));
    }

    #[test]
    fn skip_blocks_unfull() {
        let mut cache = new_cache("skip_blocks_unfull");
        // Fill 10 pages
        for i in 0..10 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([((j as usize) % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }
        // Add Unfull page
        assert_eq!(cache.store_block(Block([110; BLOCKSIZE])), PageId(10));
        cache.store_in_place(PageId(10), BlockId(1), Block([111; BLOCKSIZE]));
        cache.store_in_place(PageId(10), BlockId(2), Block([112; BLOCKSIZE]));
        cache.store_in_place(PageId(10), BlockId(3), Block([113; BLOCKSIZE]));
        cache.store_in_place(PageId(10), BlockId(4), Block([114; BLOCKSIZE]));
        cache.store_in_place(PageId(10), BlockId(5), Block([115; BLOCKSIZE]));
        assert_eq!(cache.flush_unfull(PageId(10), BlockId(6)),
                   UnfullPage::new(PageId(10), BlockId(1), BlockId(7)));
        let mut iter =
            BlockIter::new(&cache,
                           Pages((0..10).map(|i| PageId(i)).collect::<Vec<_>>(),
                                 Some(UnfullPage::new(PageId(10), BlockId(1), BlockId(6)))));

        assert_eq!(iter.next(), Some(Block([0; BLOCKSIZE])));
        iter.skip_blocks(1);
        assert_eq!(iter.next(), Some(Block([2; BLOCKSIZE])));
        iter.skip_blocks(63);
        // because on new page
        assert_eq!(iter.next(), Some(Block([2; BLOCKSIZE])));
        iter.skip_blocks(573);
        // Last page
        // 641
        assert_eq!(iter.next(), Some(Block([110; BLOCKSIZE])));
        iter.skip_blocks(1);
        assert_eq!(iter.next(), Some(Block([112; BLOCKSIZE])));
        assert_eq!(iter.next(), Some(Block([113; BLOCKSIZE])));
        iter.skip_blocks(200);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let mut iter =
            BlockIter::new(&cache,
                           Pages((0..10).map(|i| PageId(i)).collect::<Vec<_>>(),
                                 Some(UnfullPage::new(PageId(10), BlockId(1), BlockId(6)))));
        iter.skip_blocks(646);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let mut iter =
            BlockIter::new(&cache,
                           Pages((0..10).map(|i| PageId(i)).collect::<Vec<_>>(),
                                 Some(UnfullPage::new(PageId(10), BlockId(1), BlockId(6)))));
        iter.skip_blocks(639);
        assert_eq!(iter.next(), Some(Block([63; BLOCKSIZE])));
        iter.skip_blocks(1);
        assert_eq!(iter.next(), Some(Block([111; BLOCKSIZE])));
    }

}
