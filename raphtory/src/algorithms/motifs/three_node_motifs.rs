const INCOMING: usize = 0;
const OUTGOING: usize = 1;
const DIRS2D: [(usize, usize); 4] = [(0, 0), (0, 1), (1, 0), (1, 1)];

fn map2d(d1: usize, d2: usize) -> usize {
    2 * d1 + d2
}

fn map3d(d1: usize, d2: usize, d3: usize) -> usize {
    4 * d1 + 2 * d2 + d3
}

// Two Node Motifs
pub struct TwoNodeEvent {
    pub dir: usize,
    pub time: i64,
}
pub struct TwoNodeCounter {
    count1d: [usize; 2],
    count2d: [usize; 4],
    pub count3d: [usize; 8],
}

pub fn two_node_event(dir: usize, time: i64) -> TwoNodeEvent {
    TwoNodeEvent { dir, time }
}

impl TwoNodeCounter {
    pub fn execute(&mut self, events: &Vec<TwoNodeEvent>, delta: i64) {
        let mut start = 0;
        for event in events.iter() {
            while events[start].time + delta < event.time {
                self.decrement_counts(events[start].dir);
                start += 1;
            }
            self.increment_counts(event.dir);
        }
    }

    fn decrement_counts(&mut self, dir: usize) {
        self.count1d[dir] -= 1;
        self.count2d[map2d(dir, INCOMING)] -= self.count1d[INCOMING];
        self.count2d[map2d(dir, OUTGOING)] -= self.count1d[OUTGOING];
    }

    fn increment_counts(&mut self, dir: usize) {
        // 3d counter
        for (d1, d2) in DIRS2D {
            self.count3d[map3d(d1, d2, dir)] += self.count2d[map2d(d1, d2)];
        }

        // 2d counter
        self.count2d[map2d(INCOMING, dir)] += self.count1d[INCOMING];
        self.count2d[map2d(OUTGOING, dir)] += self.count1d[OUTGOING];
        // 1d counter
        self.count1d[dir] += 1;
    }

    pub fn return_counts(&self) -> [usize; 8] {
        self.count3d
    }
}

pub fn init_two_node_count() -> TwoNodeCounter {
    TwoNodeCounter {
        count1d: [0; 2],
        count2d: [0; 4],
        count3d: [0; 8],
    }
}

// Star Motifs
pub struct StarEvent {
    nb: usize,
    dir: usize,
    pub time: i64,
}

pub fn star_event(nb: usize, dir: usize, time: i64) -> StarEvent {
    StarEvent { nb, dir, time }
}

pub struct StarCounter {
    n: usize,
    pre_nodes: Vec<usize>,
    post_nodes: Vec<usize>,
    pre_sum: [usize; 8],
    mid_sum: [usize; 8],
    post_sum: [usize; 8],
    count_pre: [usize; 8],
    count_mid: [usize; 8],
    count_post: [usize; 8],
}
impl StarCounter {
    fn push_pre(&mut self, cur_edge: &StarEvent) {
        self.pre_sum[map2d(INCOMING, cur_edge.dir)] +=
            self.pre_nodes[INCOMING * self.n + cur_edge.nb];
        self.pre_sum[map2d(OUTGOING, cur_edge.dir)] +=
            self.pre_nodes[OUTGOING * self.n + cur_edge.nb];
        self.pre_nodes[cur_edge.dir * self.n + cur_edge.nb] += 1;
    }

    fn push_post(&mut self, cur_edge: &StarEvent) {
        self.post_sum[map2d(INCOMING, cur_edge.dir)] +=
            self.post_nodes[INCOMING * self.n + cur_edge.nb];
        self.post_sum[map2d(OUTGOING, cur_edge.dir)] +=
            self.post_nodes[OUTGOING * self.n + cur_edge.nb];
        self.post_nodes[cur_edge.dir * self.n + cur_edge.nb] += 1;
    }

    fn pop_pre(&mut self, cur_edge: &StarEvent) {
        self.pre_nodes[cur_edge.dir * self.n + cur_edge.nb] -= 1;
        self.pre_sum[map2d(cur_edge.dir, INCOMING)] -=
            self.pre_nodes[INCOMING * self.n + cur_edge.nb];
        self.pre_sum[map2d(cur_edge.dir, OUTGOING)] -=
            self.pre_nodes[OUTGOING * self.n + cur_edge.nb];
    }

    fn pop_post(&mut self, cur_edge: &StarEvent) {
        self.post_nodes[cur_edge.dir * self.n + cur_edge.nb] -= 1;
        self.post_sum[map2d(cur_edge.dir, INCOMING)] -=
            self.post_nodes[INCOMING * self.n + cur_edge.nb];
        self.post_sum[map2d(cur_edge.dir, OUTGOING)] -=
            self.post_nodes[OUTGOING * self.n + cur_edge.nb];
    }

    fn process_current(&mut self, cur_edge: &StarEvent) {
        self.mid_sum[map2d(INCOMING, cur_edge.dir)] -=
            self.pre_nodes[INCOMING * self.n + cur_edge.nb];
        self.mid_sum[map2d(OUTGOING, cur_edge.dir)] -=
            self.pre_nodes[OUTGOING * self.n + cur_edge.nb];

        for (d1, d2) in DIRS2D {
            self.count_pre[map3d(d1, d2, cur_edge.dir)] += self.pre_sum[map2d(d1, d2)];
            self.count_post[map3d(cur_edge.dir, d1, d2)] += self.post_sum[map2d(d1, d2)];
            self.count_mid[map3d(d1, cur_edge.dir, d2)] += self.mid_sum[map2d(d1, d2)];
        }

        self.mid_sum[map2d(cur_edge.dir, INCOMING)] +=
            self.post_nodes[INCOMING * self.n + cur_edge.nb];
        self.mid_sum[map2d(cur_edge.dir, OUTGOING)] +=
            self.post_nodes[OUTGOING * self.n + cur_edge.nb];
    }

    pub fn execute(&mut self, edges: &Vec<StarEvent>, delta: i64) {
        let l = edges.len();
        if l < 3 {
            return;
        }
        let mut start = 0;
        let mut end = 0;
        for j in 0..l {
            while start < l && edges[start].time + delta < edges[j].time {
                self.pop_pre(&edges[start]);
                start += 1;
            }
            while (end < l) && edges[end].time <= edges[j].time + delta {
                self.push_post(&edges[end]);
                end += 1;
            }
            self.pop_post(&edges[j]);
            self.process_current(&edges[j]);
            self.push_pre(&edges[j])
        }
    }

    pub fn return_counts(&self) -> [usize; 24] {
        let mut counts = [0; 24];
        for i in 0..8 {
            counts[i] = self.count_pre[i];
            counts[8 + i] = self.count_mid[i];
            counts[16 + i] = self.count_post[i];
        }
        counts
    }
}
pub fn init_star_count(n: usize) -> StarCounter {
    StarCounter {
        n,
        pre_nodes: vec![0; 2 * n],
        post_nodes: vec![0; 2 * n],
        pre_sum: [0; 8],
        mid_sum: [0; 8],
        post_sum: [0; 8],
        count_pre: [0; 8],
        count_mid: [0; 8],
        count_post: [0; 8],
    }
}

// Triangle Motifs
pub struct TriangleEdge {
    uv_edge: bool,
    uorv: usize,
    nb: usize,
    dir: usize,
    pub time: i64,
}

pub fn new_triangle_edge(
    uv_edge: bool,
    uorv: usize,
    nb: usize,
    dir: usize,
    time: i64,
) -> TriangleEdge {
    TriangleEdge {
        uv_edge,
        uorv,
        nb,
        dir,
        time,
    }
}

pub struct TriangleCounter {
    n: usize,
    pre_nodes: Vec<usize>,
    post_nodes: Vec<usize>,
    pre_sum: [usize; 8],
    mid_sum: [usize; 8],
    post_sum: [usize; 8],
    final_counts: [usize; 8],
}
impl TriangleCounter {
    pub fn execute(&mut self, edges: &Vec<TriangleEdge>, delta: i64) {
        let l = edges.len();
        if l < 3 {
            return;
        }
        let mut start = 0;
        let mut end = 0;
        for j in 0..l {
            while start < l && edges[start].time + delta < edges[j].time {
                self.pop_pre(&edges[start]);
                start += 1;
            }
            while (end < l) && edges[end].time <= edges[j].time + delta {
                self.push_post(&edges[end]);
                end += 1;
            }
            self.pop_post(&edges[j]);
            self.process_current(&edges[j]);
            self.push_pre(&edges[j])
        }
    }

    fn push_pre(&mut self, cur_edge: &TriangleEdge) {
        let (is_uor_v, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.pre_sum[map3d(1 - is_uor_v, INCOMING, dir)] +=
                self.pre_nodes[self.n * map2d(INCOMING, 1 - is_uor_v) + nb];
            self.pre_sum[map3d(1 - is_uor_v, OUTGOING, dir)] +=
                self.pre_nodes[self.n * map2d(OUTGOING, 1 - is_uor_v) + nb];
            self.pre_nodes[self.n * map2d(dir, is_uor_v) + nb] += 1;
        }
    }

    fn push_post(&mut self, cur_edge: &TriangleEdge) {
        let (is_uor_v, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.post_sum[map3d(1 - is_uor_v, INCOMING, dir)] +=
                self.post_nodes[self.n * map2d(INCOMING, 1 - is_uor_v) + nb];
            self.post_sum[map3d(1 - is_uor_v, OUTGOING, dir)] +=
                self.post_nodes[self.n * map2d(OUTGOING, 1 - is_uor_v) + nb];
            self.post_nodes[self.n * map2d(dir, is_uor_v) + nb] += 1;
        }
    }

    fn pop_pre(&mut self, cur_edge: &TriangleEdge) {
        let (is_uor_v, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.pre_nodes[self.n * map2d(dir, is_uor_v) + nb] -= 1;
            self.pre_sum[map3d(is_uor_v, dir, INCOMING)] -=
                self.pre_nodes[self.n * map2d(INCOMING, 1 - is_uor_v)];
            self.pre_sum[map3d(is_uor_v, dir, OUTGOING)] -=
                self.pre_nodes[self.n * map2d(OUTGOING, 1 - is_uor_v)];
        }
    }

    fn pop_post(&mut self, cur_edge: &TriangleEdge) {
        let (is_uor_v, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.post_nodes[self.n * map2d(dir, is_uor_v) + nb] -= 1;
            self.post_sum[map3d(is_uor_v, dir, INCOMING)] -=
                self.post_nodes[self.n * map2d(INCOMING, 1 - is_uor_v)];
            self.post_sum[map3d(is_uor_v, dir, OUTGOING)] -=
                self.post_nodes[self.n * map2d(OUTGOING, 1 - is_uor_v)];
        }
    }

    fn process_current(&mut self, cur_edge: &TriangleEdge) {
        let (is_uor_v, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.mid_sum[map3d(1 - is_uor_v, INCOMING, dir)] -=
                self.pre_nodes[self.n * map2d(INCOMING, 1 - is_uor_v) + nb];
            self.mid_sum[map3d(1 - is_uor_v, OUTGOING, dir)] -=
                self.pre_nodes[self.n * map2d(OUTGOING, 1 - is_uor_v) + nb];
            self.mid_sum[map3d(is_uor_v, dir, INCOMING)] +=
                self.post_nodes[self.n * map2d(INCOMING, 1 - is_uor_v) + nb];
            self.mid_sum[map3d(is_uor_v, dir, OUTGOING)] +=
                self.post_nodes[self.n * map2d(OUTGOING, 1 - is_uor_v) + nb];
        } else {
            self.final_counts[0] += self.mid_sum[map3d(dir, 0, 0)]
                + self.post_sum[map3d(dir, 0, 1)]
                + self.pre_sum[map3d(1 - dir, 1, 1)];
            self.final_counts[4] += self.mid_sum[map3d(dir, 1, 0)]
                + self.post_sum[map3d(1 - dir, 0, 1)]
                + self.pre_sum[map3d(1 - dir, 0, 1)];
            self.final_counts[2] += self.mid_sum[map3d(1 - dir, 0, 0)]
                + self.post_sum[map3d(dir, 1, 1)]
                + self.pre_sum[map3d(1 - dir, 1, 0)];
            self.final_counts[6] += self.mid_sum[map3d(1 - dir, 1, 0)]
                + self.post_sum[map3d(1 - dir, 1, 1)]
                + self.pre_sum[map3d(1 - dir, 0, 0)];
            self.final_counts[1] += self.mid_sum[map3d(dir, 0, 1)]
                + self.post_sum[map3d(dir, 0, 0)]
                + self.pre_sum[map3d(dir, 1, 1)];
            self.final_counts[5] += self.mid_sum[map3d(dir, 1, 1)]
                + self.post_sum[map3d(1 - dir, 0, 0)]
                + self.pre_sum[map3d(dir, 0, 1)];
            self.final_counts[3] += self.mid_sum[map3d(1 - dir, 0, 1)]
                + self.post_sum[map3d(dir, 1, 0)]
                + self.pre_sum[map3d(dir, 1, 0)];
            self.final_counts[7] += self.mid_sum[map3d(1 - dir, 1, 1)]
                + self.post_sum[map3d(1 - dir, 1, 0)]
                + self.pre_sum[map3d(dir, 0, 0)];
        }
    }

    pub fn return_counts(&self) -> &[usize; 8] {
        &self.final_counts
    }
}
pub fn init_tri_count(n: usize) -> TriangleCounter {
    TriangleCounter {
        n: n,
        pre_nodes: vec![0; 4 * n],
        post_nodes: vec![0; 4 * n],
        pre_sum: [0; 8],
        mid_sum: [0; 8],
        post_sum: [0; 8],
        final_counts: [0; 8],
    }
}

#[cfg(test)]
mod three_node_motifs_test {
    use super::{
        init_tri_count, map2d, TriangleEdge, TwoNodeCounter, TwoNodeEvent, INCOMING, OUTGOING,
    };

    #[test]
    fn map_test() {
        assert_eq!(map2d(1, 1), 3);
    }

    #[test]
    fn two_node_test() {
        let events = vec![
            TwoNodeEvent {
                dir: OUTGOING,
                time: 1,
            },
            TwoNodeEvent {
                dir: INCOMING,
                time: 2,
            },
            TwoNodeEvent {
                dir: INCOMING,
                time: 3,
            },
        ];
        let mut twonc = TwoNodeCounter {
            count1d: [0; 2],
            count2d: [0; 4],
            count3d: [0; 8],
        };
        twonc.execute(&events, 5);
        println!("motifs are {:?}", twonc.count3d);
    }

    #[test]
    fn triad_test() {
        let events = vec![(true, 0, 1, 1, 1), (false, 1, 0, 1, 2), (false, 0, 0, 0, 3)]
            .iter()
            .map(|x| TriangleEdge {
                uv_edge: x.0,
                uorv: x.1,
                nb: x.2,
                dir: x.3,
                time: x.4,
            })
            .collect::<Vec<_>>();
        let mut triangle_count = init_tri_count(3);
        triangle_count.execute(&events, 5);
        println!("triangle motifs are {:?}", triangle_count.final_counts);
    }
}
