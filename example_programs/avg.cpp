// Author: Wes Kendall
// Copyright 2012 www.mpitutorial.com
// This code is provided freely with the tutorials on mpitutorial.com. Feel
// free to modify it for your own use. Any distribution of the code must
// either provide a link to www.mpitutorial.com or keep this header intact.
//
// Program that computes the average of an array of elements in parallel using
// scatter and gather
//
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include <Communicator.h>

#include "avg.hpp"
#include "harness.hpp"

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

static uint32_t avg(void *args, uint32_t size, void *res) {
    avg_input *input = static_cast<avg_input *>(args);
    avg_output *output = static_cast<avg_output *>(res);

    int world_rank = input->function_id;
    int world_size = input->world_size;
    int num_elements_per_proc = input->num_elements_per_proc;
    output->function_id = world_rank;

    FMI::Communicator comm(world_rank, world_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();

    // Create a random array of elements on the root process. Its total
    // size will be the number of elements per process times the number
    // of processes
    srand(input->random_seed);
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
    output->subset_avg = sub_avg[0];

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
        printf("Avg of all elements is %f\n", avg);
        fflush(stdout);
        // Compute the average across the original data for comparison
        double original_data_avg = compute_avg(rand_nums, num_elements_per_proc * world_size);
        printf("Avg computed across original data is %f\n", original_data_avg);
        fflush(stdout);

        output->total_avg = avg;
        output->globally_correct = std::fabs(avg - original_data_avg) < 0.0001;
    } else {
        output->total_avg = 0.;
        output->globally_correct = false;
    }

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(avg_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;

    return fmi_examples::run(argc, argv, "avg", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "avg";
        spec.input_size = sizeof(avg_input);
        spec.output_size = sizeof(avg_output);
        spec.get_context = [] { return static_cast<void *>(_avg::get_context()); };
        spec.free_context = [](void *ctx) { _avg::free_context(ctx); };
        spec.initialize_input = [](int func_num, int numcores, void *ctx, char *ptr) {
            _avg::initialize_input(func_num, numcores, ctx, ptr);
        };
        spec.check_output = [](int func_num, int numcores, void *ctx, char *ptr) {
            return _avg::check_output(func_num, numcores, ctx, ptr);
        };
        spec.fn = avg;
        return spec;
    });
}
