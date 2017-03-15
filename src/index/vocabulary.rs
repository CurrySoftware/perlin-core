use std::hash::Hash;
use std::collections::HashMap;
use std::collections::hash_map::Keys;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct TermId(pub u64);

#[derive(Debug, Clone)]
pub struct SharedVocabulary<TTerm: Hash + Eq>(HashMap<TTerm, TermId>);

impl<TTerm: Hash + Eq> SharedVocabulary<TTerm> {
    pub fn new() -> Self {
        SharedVocabulary(HashMap::new())
    }
}

pub trait TermIterator<'a, TTerm: 'a> {
    type TIter: Iterator<Item=&'a TTerm>;
    fn iterate_terms(&'a self) -> Self::TIter;
}

pub trait Vocabulary<TTerm> {
    fn get_or_add(&mut self, TTerm) -> TermId;
    fn get(&self, &TTerm) -> Option<TermId>;
}

impl<'a, TTerm: 'a + Hash + Eq> TermIterator<'a, TTerm> for SharedVocabulary<TTerm> {
    type TIter = Keys<'a, TTerm, TermId>;

    fn iterate_terms(&'a self) -> Self::TIter {
        self.0.keys()
    }
}

impl<TTerm: Hash + Eq> Vocabulary<TTerm> for SharedVocabulary<TTerm>{
    fn get_or_add(&mut self, term: TTerm) -> TermId {
        {//Scope of read lock            
            if let Some(term_id) = self.0.get(&term) {
                return *term_id;
            }            
        }
        {
            //between last time checking and write locking, the term could have already been added!
            if let Some(term_id) = self.0.get(&term) {
                return *term_id;
            }
            //It was obivously not added. so we will do this now!
            let term_id = TermId(self.0.len() as u64);
            self.0.insert(term, term_id);
            term_id
        }
    }

    fn get(&self, term: &TTerm) -> Option<TermId> {
        self.0.get(term).cloned()
    }
}

impl<TTerm> Vocabulary<TTerm> for HashMap<TTerm, TermId> where TTerm: Hash + Eq{
    fn get_or_add(&mut self, term: TTerm) -> TermId {
        let len = self.len();
        *self.entry(term).or_insert_with(|| TermId(len as u64))
    }

    #[inline]
    fn get(&self, term: &TTerm) -> Option<TermId> {
        self.get(term).cloned()
    }
}
