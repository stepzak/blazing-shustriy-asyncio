use std::time::Instant;

use crate::core::task::TaskId;

pub struct Timer {
    pub when: Instant,
    pub task_id: TaskId,
}

impl Eq for Timer {}
impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
    }
}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.when.partial_cmp(&self.when)
    }
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.when.cmp(&self.when)
    }
}
