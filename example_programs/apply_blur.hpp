#pragma once

#include <cstdio>
#include <stdexcept>
#include <string>
#include <string_view>

#include "util.hpp"

struct apply_blur_input {
    int function_id;
    char input_path[100];
    char output_prefix[100];
};

struct apply_blur_output {
    int function_id;
    char output_path[100];
};

namespace _apply_blur {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *context) {}

    // Fills `buffer` with printf-formatted text, throwing instead of silently truncating.
    // The GapRunner original relies on fixed layout paths that always fit; here the
    // directories are configurable, so an overflow has to be reported.
    template <size_t N, typename... Args>
    inline void format_path(char (&buffer)[N], const char *what, const char *format, Args... args) {
        int written = snprintf(buffer, N, format, args...);
        if (written < 0) {
            throw std::runtime_error(std::string("apply_blur: failed to format ") + what);
        }
        if (static_cast<size_t>(written) >= N) {
            throw std::runtime_error(std::string("apply_blur: ") + what + " does not fit in " + std::to_string(N - 1) +
                                     " characters (needs " + std::to_string(written) + ")");
        }
    }

    inline void initialize_input(int func_num, int numcores, const std::string &input_dir,
                                 const std::string &output_dir, char *ptr) {
        apply_blur_input *args = reinterpret_cast<apply_blur_input *>(ptr);
        args->function_id = func_num;
        format_path(args->input_path, "input path", "%s/input_%d.jpg", input_dir.c_str(), func_num);
        format_path(args->output_prefix, "output prefix", "%s", output_dir.c_str());
    }

    inline bool check_output(int func_num, int numcores, const std::string &output_dir, char *ptr) {
        apply_blur_output *res = reinterpret_cast<apply_blur_output *>(ptr);

        bool correct = compare(func_num, "function_id", func_num, res->function_id);

        char expected_output_path[100];
        format_path(expected_output_path, "expected output path", "%s/output_func_%d.jpg", output_dir.c_str(),
                    func_num);
        correct &= compare(func_num, "output_path", std::string_view(expected_output_path),
                           std::string_view(res->output_path));

        return correct;
    }

} // namespace _apply_blur
