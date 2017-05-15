use std::cmp::Ordering;

#[derive(PartialEq, PartialOrd, Debug)]
pub struct Progress(f32);

impl Progress {
    pub fn done() -> Self {
        Progress(1f32)
    }
    
    pub fn from(a: u32, b: u32) -> Self {     
        if b == 0 {
            Progress::done()
        } else {
            Progress((a as f32/b as f32))
        }
    }

    pub fn project_amount(&self, until_now: u32) -> u32 {
        (until_now as f32 * 1f32/self.0) as u32
    }
}

impl Eq for Progress {}

impl Ord for Progress {
    fn cmp(&self, other: &Progress) -> Ordering {
        self.partial_cmp(other).unwrap()
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

    #[test]
    fn sub_percent(){
        //0.001% done
        let progress = Progress::from(1, 100_000);
        assert_eq!(progress.project_amount(1), 100_000);
        assert_eq!(progress.project_amount(10), 1_000_000);
        assert_eq!(progress.project_amount(20), 2_000_000);
    }

    #[test]
    fn full() {
        let progress = Progress::from(10, 10);
        assert_eq!(progress.project_amount(10), 10);
    }
    
}
