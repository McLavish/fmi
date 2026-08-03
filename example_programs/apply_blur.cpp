#include <cstdint>
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>

#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

#include "apply_blur.hpp"
#include "harness.hpp"

static uint32_t apply_blur(void *args, uint32_t size, void *res) {
    apply_blur_input *in = static_cast<apply_blur_input *>(args);
    apply_blur_output *out = static_cast<apply_blur_output *>(res);

    out->function_id = in->function_id;
    _apply_blur::format_path(out->output_path, "output path", "%s/output_func_%d.jpg", in->output_prefix,
                             out->function_id);

    cv::Mat input_image = cv::imread(in->input_path, cv::IMREAD_COLOR);
    if (input_image.empty()) {
        throw std::runtime_error(std::string("apply_blur: could not read input image '") + in->input_path + "'");
    }
    cv::Mat output_image(input_image.size(), input_image.type());

    cv::GaussianBlur(input_image, output_image, cv::Size_<int>(13, 13), 13, 13);
    if (!cv::imwrite(out->output_path, output_image)) {
        throw std::runtime_error(std::string("apply_blur: could not write output image '") + out->output_path +
                                 "' (does the output directory exist?)");
    }

    std::cout << "Function with id=" << in->function_id << " applied Gaussian blur to image " << in->input_path
              << " and wrote resulting image to " << out->output_path << std::endl;

    return sizeof(apply_blur_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_string("input-dir", "example_programs/apply_blur_inputs",
                     "Directory holding the per-rank input_<rank>.jpg images");
    flags.add_string("output-dir", "example_programs/apply_blur_outputs",
                     "Directory the blurred output_func_<rank>.jpg images are written to");

    return fmi_examples::run(argc, argv, "apply_blur", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "apply_blur";
        spec.input_size = sizeof(apply_blur_input);
        spec.output_size = sizeof(apply_blur_output);
        spec.get_context = [] { return _apply_blur::get_context(); };
        spec.free_context = [](void *ctx) { _apply_blur::free_context(ctx); };
        std::string input_dir = opts.flags.get_string("input-dir");
        std::string output_dir = opts.flags.get_string("output-dir");
        spec.initialize_input = [input_dir, output_dir](int func_num, int numcores, void *, char *ptr) {
            _apply_blur::initialize_input(func_num, numcores, input_dir, output_dir, ptr);
        };
        spec.check_output = [output_dir](int func_num, int numcores, void *, char *ptr) {
            return _apply_blur::check_output(func_num, numcores, output_dir, ptr);
        };
        spec.fn = apply_blur;
        return spec;
    });
}
