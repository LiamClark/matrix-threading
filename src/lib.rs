#![feature(test)]
extern crate test;
extern crate crossbeam;
extern crate rayon;
extern crate scoped_threadpool;
use scoped_threadpool::Pool;
use std::sync::mpsc::channel;

use rayon::prelude::*;


fn sum_thread_pool(pool: &mut Pool, matrix: &Vec<Vec<i32>>, thread_count: usize) -> i32 {
    let chunk_size = matrix.len().checked_div(thread_count).unwrap();
    let matrix_chunks: Vec<&[Vec<i32>]> = matrix.chunks(chunk_size).collect();

    let (tx, rx) = channel();
    for data in matrix_chunks {
        let thread_tx = tx.clone();
        pool.scoped(|scope| {
            scope.execute(move || {
                let mut sum = 0;
                for vec in data {
                    for i in vec {
                        if i % 2 != 0 {
                            sum += 1;
                        }
                    }
                }
                thread_tx.send(sum).unwrap()
            });
        });
    }
    let mut sum = 0;
    for _ in 0..thread_count {
            sum += rx.recv().unwrap()
    }

    sum
}

fn sum_odd_serial(matrix: &Vec<Vec<i32>>) -> i32 {
    let mut sum = 0;
    for vec in matrix {
        for i in vec {
            if i % 2 != 0 {
                sum += 1;
            }
        }
    }

    sum
}

fn sum_odd_functional(matrix: &Vec<Vec<i32>>) -> i32 {
    matrix.into_iter()
        .flat_map(|v| v)
        .filter(|i| *i % 2 != 0)
        .count() as i32
}


fn sum_parallel_bad_scoped(matrix: &Vec<Vec<i32>>, thread_count: usize) -> i32 {
    let mut counters = vec!(0, 0, 0, 0);
    let mut threads = Vec::with_capacity(thread_count);

    let chunk_size = matrix.len().checked_div(thread_count).unwrap();
    let matrix_chunks: Vec<&[Vec<i32>]> = matrix.chunks(chunk_size).collect();

    for thread_no in 0..thread_count {
        let data = matrix_chunks[thread_no];
        crossbeam::scope(|scope| {
            let thread = scope.spawn(|| {
                for vec in data {
                    for i in vec {
                        if i % 2 != 0 {
                            counters[thread_no] += 1;
                        }
                    }
                }
            });

            threads.push(thread);
        });
    }

    for thread in threads {
        thread.join();
    }

    let mut sum = 0;

    for count in counters {
        sum += count;
    }

    sum
}

fn sum_parallel_good(matrix: &Vec<Vec<i32>>, thread_count: usize) -> i32 {
    let mut threads = Vec::with_capacity(thread_count);

    let chunk_size = matrix.len().checked_div(thread_count).unwrap();
    let matrix_chunks: Vec<&[Vec<i32>]> = matrix.chunks(chunk_size).collect();


    for thread_no in 0..thread_count {
        let data = matrix_chunks[thread_no];
        crossbeam::scope(|scope| {
            let thread = scope.spawn(|| {
                // println!("{:?}", "thread started" );
                let mut sum = 0;
                       for vec in data {
                           for i in vec {
                               if i % 2 != 0 {
                                   sum += 1;
                               }
                           }
                       }

                       sum
            });

            threads.push(thread);
        });
    }

    // println!("done looping");

    threads.into_iter()
        .map(|thread| thread.join())
        .sum()
}

fn sum_rayon(matrix: &Vec<Vec<i32>>) -> i32 {
    matrix.par_iter()
            .map(|v| {
                let mut sum = 0;
                for i in v {
                    if i % 2 != 0 {
                        sum += 1;
                    }
                }

                sum
            }).sum()
}

fn sum_rayon_flat(matrix: &Vec<Vec<i32>>) -> i32 {
    matrix.par_iter()
            .flat_map(|v| v)
            .filter(|i| *i % 2 != 0)
            .map(|i| *i)
            .sum()
}

#[cfg(test)]
mod tests {
    use sum_odd_functional;
    use sum_odd_serial;
    use sum_parallel_bad_scoped;
    use sum_parallel_good;
    use sum_rayon;
    use sum_rayon_flat;
    use sum_thread_pool;
    use test::Bencher;
    use scoped_threadpool::Pool;

    static N: i32 = 1000;

    #[bench]
    fn bench_procedural(b: &mut Bencher) {
        let mat = giant_matrix(N as usize);

        b.iter(|| sum_odd_serial(&mat))
    }

    #[bench]
    fn bench_functional(b: &mut Bencher) {
        let mat = giant_matrix(N as usize);

        b.iter(|| sum_odd_functional(&mat))
    }

    #[bench]
    fn bench_parallel_sharing(b: &mut Bencher) {
        let mat = giant_matrix(N as usize);

        b.iter(|| sum_parallel_bad_scoped(&mat, 4))
    }

    #[bench]
    fn bench_parallel_cute(b: &mut Bencher) {
        let mat = giant_matrix(N as usize);

        b.iter(|| {

            sum_parallel_good(&mat, 8)
        })
    }

    #[bench]
    fn bench_rayon(b: &mut Bencher) {
        let mat = giant_matrix(N as usize);

        b.iter(|| {

            sum_rayon(&mat)
        })
    }

    #[bench]
    fn bench_rayon_flat(b: &mut Bencher) {
        let mat = giant_matrix(N as usize);

        b.iter(|| {
            sum_rayon_flat(&mat)
        })
    }

    #[bench]
    fn bench_pool(b: &mut Bencher) {
        let matrix = giant_matrix(N as usize);
        let mut pool = Pool::new(4);

        b.iter(|| {
            sum_thread_pool(&mut pool, &matrix, 4);
        })
    }

    fn giant_matrix(size: usize) -> Vec<Vec<i32>> {
        let mut outer = Vec::with_capacity(size);
        for i in 0..size {
            let mut inner = Vec::with_capacity(size);

            for j in 0..size {
                inner.push(i as i32);
            }
            outer.push(inner);
        }

        outer
    }



    fn small_matrix() -> Vec<Vec<i32>> {
        vec!(
            vec!(1, 2, 3, 4, 5, 6),
            vec!(1, 2, 3, 4, 5, 6),
        )
    }

    #[test]
    fn it_works() {
        let matrix = small_matrix();
        assert_eq!(6, sum_odd_serial(&matrix));
    }

    #[test]
    fn test_funct() {
        let matrix = small_matrix();
        assert_eq!(6, sum_odd_functional(&matrix));
    }

    #[test]
    fn test_bad_parralel() {
        let matrix = small_matrix();
        assert_eq!(6, sum_parallel_bad_scoped(&matrix, 2));
    }

    #[test]
    fn test_good_parralel() {
        let matrix = small_matrix();
        assert_eq!(6, sum_parallel_good(&matrix, 2));
    }

    #[test]
    fn test_rayon() {
        let matrix = small_matrix();
        assert_eq!(6, sum_rayon(&matrix));
    }

    #[test]
    fn test_thread_pool() {
        let matrix = small_matrix();
        let mut pool = Pool::new(2);
        assert_eq!(6, sum_thread_pool(&mut pool, &matrix, 2));
    }
}
