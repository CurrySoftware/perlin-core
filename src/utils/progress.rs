#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub struct Progress(u16);

impl Progress {
    pub fn done() -> Self {
        Progress(10_000)
    }
    
    pub fn from(a: u32, b: u32) -> Self {
        if b == 0 {
            Progress::done()
        } else {
            Progress(((a * 10_000)/b) as u16)
        }
    }

    pub fn project_amount(&self, until_now: u32) -> u32 {
        until_now * (10000/self.0 as u32)
    }
}


#[cfg(test)]
mod tests {
    use super::Progress;

    #[test]
    fn basic(){
        //10% done
        let progress = Progress::from(10, 100);
        //10% yielded 10 results -> 100% yield 100
        assert_eq!(progress.project_amount(10), 100);
        assert_eq!(progress.project_amount(1), 10);
        assert_eq!(progress.project_amount(20), 200);
    }
}
