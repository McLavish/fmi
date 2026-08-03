// Author: Wes Kendall
// Copyright 2012 www.mpitutorial.com
// This code is provided freely with the tutorials on mpitutorial.com. Feel
// free to modify it for your own use. Any distribution of the code must
// either provide a link to www.mpitutorial.com or keep this header intact.
//
// Program that computes the average of an array of elements in parallel using
// scatter and gather
//
#include <Communicator.h>

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "launcher.hpp"
#include "util.hpp"

constexpr int SEED = 42;
constexpr int NUM_ELEMENTS_PER_PROC = 1000;

// Creates an array of random numbers. Each number has a value from 0 - 1
static std::vector<double> create_rand_nums(int num_elements) {
    std::vector<double> rand_nums(num_elements);
    int i;
    for (i = 0; i < num_elements; i++) {
        rand_nums[i] = 100 * (rand() / (double)RAND_MAX);
    }
    return rand_nums;
}

// Computes the average of an array of numbers
static double compute_avg(std::vector<double> array, int num_elements) {
    double sum = 0.;
    int i;
    for (i = 0; i < num_elements; i++) {
        sum += array[i];
    }
    return sum / num_elements;
}

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "avg", opts, exit_code)) {
        return exit_code;
    }

    // No example-specific guard: every rank count >= 1 is valid here. Rank 0 sizes the source
    // array as NUM_ELEMENTS_PER_PROC * ranks, so the scatter is always exactly divisible, and a
    // single-rank run degenerates to a local copy that still validates.

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int world_rank = opts.rank;
    const int world_size = opts.ranks;
    const int num_elements_per_proc = NUM_ELEMENTS_PER_PROC;
    bool ok = true;

    try {
        FMI::Communicator comm(world_rank, world_size, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << world_rank << ": established communicator, " << num_elements_per_proc
                  << " elements per rank" << std::endl;

        // Create a random array of elements on the root process. Its total
        // size will be the number of elements per process times the number
        // of processes
        srand(SEED);
        std::vector<double> rand_nums;
        if (world_rank == 0) {
            rand_nums = create_rand_nums(num_elements_per_proc * world_size);
        }

        // For each process, create a buffer that will hold a subset of the entire
        // array
        std::vector<double> sub_rand_nums(num_elements_per_proc);

        // Scatter the random numbers from the root process to all processes in
        // the world
        FMI::Comm::Data<std::vector<double>> _buf_rand_nums(rand_nums), _buf_sub_rand_nums(sub_rand_nums);
        comm.scatter(_buf_rand_nums, _buf_sub_rand_nums, 0);
        sub_rand_nums = _buf_sub_rand_nums.get();

        // Compute the average of your subset
        std::vector<double> sub_avg = {compute_avg(sub_rand_nums, num_elements_per_proc)};
        std::cout << "rank " << world_rank << ": subset avg is " << sub_avg[0] << std::endl;

        // Gather all partial averages down to the root process
        std::vector<double> sub_avgs;
        if (world_rank == 0) {
            sub_avgs = std::vector<double>(world_size);
        }
        FMI::Comm::Data<std::vector<double>> _buf_sub_avg(sub_avg), _buf_sub_avgs(sub_avgs);
        comm.gather(_buf_sub_avg, _buf_sub_avgs, 0);
        sub_avgs = _buf_sub_avgs.get();

        // Now that we have all of the partial averages on the root, compute the
        // total average of all numbers. Since we are assuming each process computed
        // an average across an equal amount of elements, this computation will
        // produce the correct answer.
        if (world_rank == 0) {
            double avg = compute_avg(sub_avgs, world_size);
            std::cout << "rank " << world_rank << ": Avg of all elements is " << avg << std::endl;
            // Compute the average across the original data for comparison
            double original_data_avg = compute_avg(rand_nums, num_elements_per_proc * world_size);
            std::cout << "rank " << world_rank << ": Avg computed across original data is " << original_data_avg
                      << std::endl;

            // The root's own end-to-end verdict: the average of the gathered per-rank averages must
            // match the average over the array it scattered.
            bool globally_correct = std::fabs(avg - original_data_avg) < 0.0001;
            ok = compare(world_rank, "globally_correct", true, globally_correct);
        }

        fmi_examples::teardown_sync(comm, opts);
    } catch (const std::exception &e) {
        std::cout << "rank " << world_rank << " crashed: " << e.what() << std::endl;
        ok = false;
    } catch (const std::string &s) {
        std::cout << "rank " << world_rank << " crashed: " << s << std::endl;
        ok = false;
    } catch (...) {
        std::cout << "rank " << world_rank << " crashed: unknown exception" << std::endl;
        ok = false;
    }

    std::cout << "rank " << world_rank << ": " << (ok ? "PASS" : "FAIL") << std::endl;
    return ok ? 0 : 1;
}
