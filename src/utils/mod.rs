pub mod progress;
pub mod seeking_iterator;
pub mod counter;
pub mod ring_buffer;

pub trait Baseable<T> {
    fn add_base(&mut self, T);
    fn sub_base(&mut self, T);
}
