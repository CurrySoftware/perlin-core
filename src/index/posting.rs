use compressor::Compressor;
use page_manager::BlockIter;
use utils::ring_buffer::BiasedRingBuffer;
use utils::Baseable;
use index::listing::UsedCompressor;

const SAMPLING_THRESHOLD: usize = 200;

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Copy, Clone)]
pub struct Posting(pub DocId);
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Copy, Clone)]
pub struct DocId(pub u64);

impl DocId {
    #[inline]
    pub fn none() -> DocId {
        DocId(u64::max_value())
    }

    #[inline]
    pub fn inc(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

impl<'a> Baseable<&'a DocId> for DocId {
    #[inline]
    fn base_on(&mut self, other: &Self) {
        self.0 -= other.0
    }
}

impl Posting {
    #[inline]
    pub fn none() -> Posting {
        Posting(DocId::none())
    }

    pub fn doc_id(&self) -> DocId {
        self.0
    }
}

impl Default for Posting {
    fn default() -> Self {
        Posting(DocId(0))
    }
}

impl<'a> Baseable<&'a Posting> for Posting {
    #[inline]
    fn base_on(&mut self, other: &Self) {
        self.0.base_on(&other.0);
    }
}

/// Wraps the Decoder around an enum.
/// For the possibility of an empty decoder
pub enum PostingIterator<'a> {
    Empty,
    Decoder(PostingDecoder<'a>),
}

/// Takes a block iterator and a list of biases and iterates over the resulting
/// postings
pub struct PostingDecoder<'a> {
    blocks: BlockIter<'a>,
    bias_list: Vec<Posting>,
    bias_list_ptr: usize,
    posting_buffer: BiasedRingBuffer<Posting>,
}

impl<'a> PostingDecoder<'a> {
    pub fn new(blocks: BlockIter<'a>, bias_list: Vec<Posting>) -> Self {
        PostingDecoder {
            blocks: blocks,
            bias_list: bias_list,
            bias_list_ptr: 0,
            posting_buffer: BiasedRingBuffer::new(),
        }
    }
}

impl<'a> Iterator for PostingIterator<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        match *self {
            PostingIterator::Empty => None,
            PostingIterator::Decoder(ref mut decoder) => decoder.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match *self {
            PostingIterator::Empty => (0, Some(0)),
            PostingIterator::Decoder(ref decoder) => decoder.size_hint(),
        }
    }
}

impl<'a> ExactSizeIterator for PostingIterator<'a> { }

impl<'a> Iterator for PostingDecoder<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        if self.posting_buffer.is_empty() {
            if let Some(block) = self.blocks.next() {
                let bias = self.bias_list[self.bias_list_ptr];
                self.bias_list_ptr += 1;
                self.posting_buffer.base_on(bias);
                UsedCompressor::decompress(block, &mut self.posting_buffer);
            }
        }
        self.posting_buffer.pop_front()
    }

    //This will be wrong if either the compressor or the blocksize changes.
    //Pay attention!
    //TODO: Solve that independently of blocksize and compressor
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.bias_list.len() * 8, Some(self.bias_list.len() * 8))
    }
}

pub fn get_intersection_size(mut lhs: PostingIterator, mut rhs: PostingIterator) -> usize {
    intersection_size(&mut lhs, &mut rhs)
}


pub fn estimate_intersection_size(lhs: PostingIterator, rhs: PostingIterator) -> usize {
    //Get the shorter one
    let (mut shorter, mut longer) = if lhs.len() < rhs.len() { (lhs, rhs) } else { (rhs, lhs) };

    
    if shorter.len() < SAMPLING_THRESHOLD {
        //Count
        intersection_size(&mut shorter, &mut longer)
    } else {
        intersection_size_limit(&mut shorter, &mut longer, 100) * (shorter.len()/100)
    }
}

macro_rules! unwrap_or_break{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            break;
        }
    }
}

fn intersection_size_limit(shorter: &mut PostingIterator, longer: &mut PostingIterator, limit: usize) -> usize {
    let mut count = 0;
    for _ in 0..limit {
        let mut l = unwrap_or_break!(shorter.next());
        let mut r = unwrap_or_break!(longer.next());
        while l != r {
            if l < r {
                l = unwrap_or_break!(shorter.next());
            } else {
                r = unwrap_or_break!(longer.next());
            }
        }
        count += 1;
    }
    count
}


fn intersection_size(shorter: &mut PostingIterator, longer: &mut PostingIterator) -> usize {
    let mut count = 0;
    loop {
        let mut l = unwrap_or_break!(shorter.next());
        let mut r = unwrap_or_break!(longer.next());
        while l != r {
            if l < r {
                l = unwrap_or_break!(shorter.next());
            } else {
                r = unwrap_or_break!(longer.next());
            }
        }
        count += 1;
    }
    count
}


#[cfg(test)]
mod tests {
    use super::*;

    use index::listing::Listing;
    use page_manager::{FsPageManager, RamPageCache};


    use test_utils::create_test_dir;

    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("posting/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }

    #[test]
    fn single() {
        let mut cache = new_cache("single");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        listing.commit(&mut cache);
        assert_eq!(listing.posting_decoder(&cache).collect::<Vec<_>>(),
                   vec![Posting(DocId(0))]);
    }

    #[test]
    fn many() {
        let mut cache = new_cache("many");
        let mut listing = Listing::new();
        for i in 0..2048 {
            listing.add(&[Posting(DocId(i))], &mut cache);
        }
        listing.commit(&mut cache);
        let res = (0..2048).map(|i| Posting(DocId(i))).collect::<Vec<_>>();
        assert_eq!(listing.posting_decoder(&cache).collect::<Vec<_>>(), res);
    }

    #[test]
    fn multiple_listings() {
        let mut cache = new_cache("multiple_listings");
        let mut listing1 = Listing::new();
        let mut listing2 = Listing::new();
        let mut listing3 = Listing::new();
        for i in 0..2049 {
            listing1.add(&[Posting(DocId(i))], &mut cache);
            listing2.add(&[Posting(DocId(i * 2))], &mut cache);
            listing3.add(&[Posting(DocId(i * 3))], &mut cache);
        }
        listing1.commit(&mut cache);
        listing2.commit(&mut cache);
        listing3.commit(&mut cache);
        let res1 = (0..2049).map(|i| Posting(DocId(i))).collect::<Vec<_>>();
        let res2 = (0..2049).map(|i| Posting(DocId(i * 2))).collect::<Vec<_>>();
        let res3 = (0..2049).map(|i| Posting(DocId(i * 3))).collect::<Vec<_>>();
        assert_eq!(listing1.posting_decoder(&cache).collect::<Vec<_>>(), res1);
        assert_eq!(listing2.posting_decoder(&cache).collect::<Vec<_>>(), res2);
        assert_eq!(listing3.posting_decoder(&cache).collect::<Vec<_>>(), res3);
    }

    #[test]
    fn different_listings() {
        let mut cache = new_cache("different_listings");
        let mut listing1 = Listing::new();
        let mut listing2 = Listing::new();
        let mut listing3 = Listing::new();
        for i in 0..4596 {
            listing1.add(&[Posting(DocId(i))], &mut cache);
            if i % 2 == 0 {
                listing2.add(&[Posting(DocId(i * 2))], &mut cache);
            }
            if i % 3 == 0 {
                listing3.add(&[Posting(DocId(i * 3))], &mut cache);
            }
        }
        listing1.commit(&mut cache);
        listing2.commit(&mut cache);
        listing3.commit(&mut cache);
        let res1 = (0..4596).map(|i| Posting(DocId(i))).collect::<Vec<_>>();
        let res2 =
            (0..4596).filter(|i| i % 2 == 0).map(|i| Posting(DocId(i * 2))).collect::<Vec<_>>();
        let res3 =
            (0..4596).filter(|i| i % 3 == 0).map(|i| Posting(DocId(i * 3))).collect::<Vec<_>>();
        assert_eq!(listing1.posting_decoder(&cache).collect::<Vec<_>>(), res1);
        assert_eq!(listing2.posting_decoder(&cache).collect::<Vec<_>>(), res2);
        assert_eq!(listing3.posting_decoder(&cache).collect::<Vec<_>>(), res3);
    }

    #[test]
    fn intersection_size() {
        let mut cache = new_cache("intersection_size");
        let mut listing1 = Listing::new();
        let mut listing2 = Listing::new();
        for i in 0..100 {
            listing1.add(&[Posting(DocId(i))], &mut cache);
            listing2.add(&[Posting(DocId(i))], &mut cache);

        }
        listing1.commit(&mut cache);
        listing2.commit(&mut cache);

        assert_eq!(estimate_intersection_size(PostingIterator::Decoder(listing1.posting_decoder(&cache)), PostingIterator::Decoder(listing2.posting_decoder(&cache))), 100);
    }


}
