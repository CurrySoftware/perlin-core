pub mod seeking_iterator;
pub mod counter;
pub mod ring_buffer;
#[macro_use]
pub mod try_option;

pub trait Baseable<T> {
    fn add_base(&mut self, T);
    fn sub_base(&mut self, T);
}
