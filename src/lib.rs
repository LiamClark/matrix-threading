extern crate crossbeam;


fn sum_odd_serial(matrix: Vec<Vec<i32>>) -> i32 {
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

fn sum_odd_functional(matrix: Vec<Vec<i32>>) -> i32 {
    matrix.into_iter()
        .flat_map(|v| v)
        .filter(|i| i % 2 != 0)
        .count() as i32
}


fn sum_parallel_bad_scoped(matrix: Vec<Vec<i32>>, thread_count: usize) -> i32 {
    let mut counters = vec!(0, 0 ,0 ,0);
    let mut threads = Vec::with_capacity(thread_count);

    let chunk_size = matrix.len().checked_div(thread_count).unwrap();
    println!("chunk_size {:?}", chunk_size);
    let matrix_chunks: Vec<&[Vec<i32>]> = matrix.chunks(chunk_size).collect();
    println!("we have chunks");

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
    println!("chunk_size {:?}", chunk_size);
    let matrix_chunks: Vec<&[Vec<i32>]> = matrix.chunks(chunk_size).collect();

    println!("we have chunks");

    for thread_no in 0..thread_count {
        let data = matrix_chunks[thread_no];
        crossbeam::scope(|scope| {
            let thread = scope.spawn(|| {
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

    threads.into_iter()
        .map(|thread| thread.join())
        .sum()
}


#[cfg(test)]
mod tests {
    use sum_odd_functional;
    use sum_odd_serial;
    use sum_parallel_bad_scoped;
    use sum_parallel_good;

    fn small_matrix() -> Vec<Vec<i32>> {
        vec!(
            vec!(1, 2, 3, 4, 5, 6),
            vec!(1, 2, 3, 4, 5, 6),
        )
    }


    #[test]
    fn it_works() {
        let matrix = small_matrix();
        assert_eq!(6, sum_odd_serial(matrix));
    }

    #[test]
    fn test_funct() {
        let matrix = small_matrix();
        assert_eq!(6, sum_odd_functional(matrix));
    }

    #[test]
    fn test_bad_parralel() {
        let matrix = small_matrix();
        assert_eq!(6, sum_parallel_bad_scoped(matrix, 2));
    }

    #[test]
    fn test_good_parralel() {
        let matrix = small_matrix();
        assert_eq!(6, sum_parallel_good(matrix, 2));
    }

}
