use std::hash::Hash;
use std::mem;
use std::fmt::{Formatter, Error, Debug, Display};
 
use index::Index;
use index::listing::Listing;

impl<T: Hash + Eq> Debug for Index<T> {

    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        if f.alternate() {
            debug_verbose(self, f)
        } else {
            debug(self, f)
        }
    }

}


impl<T: Hash + Eq> Display for Index<T> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        display(self, f)
    }
}

fn display<T: Hash + Eq>(index: &Index<T>, f: &mut Formatter) -> Result<(), Error> {
    writeln!(f, "Index with {} Documents; Last DocId is {:?}", index.doc_count, index.last_doc_id)
}

fn debug<T: Hash + Eq>(index: &Index<T>, f: &mut Formatter) -> Result<(), Error> {
    writeln!(f, "Index with {} Documents; Last DocId is {:?}", index.doc_count, index.last_doc_id)?;
    writeln!(f, "\tIt has {} listings!", index.listings.len())?;
    writeln!(f, "\tThe listings heap size is {}!", index.listings.len() * mem::size_of::<Listing>()) 
}

fn debug_verbose<T: Hash + Eq>(index: &Index<T>, f: &mut Formatter) -> Result<(), Error> {
    writeln!(f, "Index with {} Documents; Last DocId is {:?}", index.doc_count, index.last_doc_id)
}
