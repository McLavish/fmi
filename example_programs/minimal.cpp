#include <cstdint>
#include <iostream>

#include "harness.hpp"
#include "minimal.hpp"

static uint32_t minimal(void *args, uint32_t size, void *res) {
    minimal_input *in = static_cast<minimal_input *>(args);
    minimal_output *out = static_cast<minimal_output *>(res);

    out->function_id = in->function_id;
    out->function_result = (1ll * in->function_payload * (in->function_payload + 1)) / 2;

    std::cout << "Function with id=" << in->function_id << " calculating sum(1.." << in->function_payload
              << ")=" << out->function_result << std::endl;

    return sizeof(minimal_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;

    return fmi_examples::run(argc, argv, "minimal", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "minimal";
        spec.input_size = sizeof(minimal_input);
        spec.output_size = sizeof(minimal_output);
        spec.get_context = [] { return _minimal::get_context(); };
        spec.free_context = [](void *ctx) { _minimal::free_context(ctx); };
        spec.initialize_input = [](int func_num, int numcores, void *ctx, char *ptr) {
            _minimal::initialize_input(func_num, numcores, ctx, ptr);
        };
        spec.check_output = [](int func_num, int numcores, void *ctx, char *ptr) {
            return _minimal::check_output(func_num, numcores, ctx, ptr);
        };
        spec.fn = minimal;
        return spec;
    });
}
