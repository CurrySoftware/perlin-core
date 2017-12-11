#[derive(Debug)]
pub struct Counter(u64);

impl Counter {

    pub fn new() -> Counter {
        Counter(0)
    }

    pub fn retrieve_and_inc(&mut self) -> u64 {
        self.0 += 1;
        self.0 - 1
    }

    pub fn retrieve(&self) -> u64 {
        self.0
    }
}
