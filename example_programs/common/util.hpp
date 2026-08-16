#pragma once

#include <cmath>
#include <iostream>
#include <string>

template <typename T>
bool compare(int func_num, const std::string &field, const T &expected, const T &got) {
    if (expected == got) {
        return true;
    }

    std::cout << "Function " << func_num << " returned wrong " << field << ": expected " << expected << ", got " << got
              << std::endl;

    return false;
}

template <>
inline bool compare<double>(int func_num, const std::string &field, const double &expected, const double &got) {
    constexpr double eps = 0.001;
    if (std::fabs(expected - got) < eps) {
        return true;
    }

    std::cout << "Function " << func_num << " returned wrong " << field << ": expected " << expected << ", got " << got
              << std::endl;

    return false;
}
