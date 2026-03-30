#ifndef FMI_PYTHONBINDINGSUPPORT_H
#define FMI_PYTHONBINDINGSUPPORT_H

#include <boost/python/list.hpp>
#include <boost/python/object.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/stl_iterator.hpp>
#include <utils/Common.h>

#include <algorithm>
#include <functional>
#include <stdexcept>
#include <vector>

namespace FMI::Utils {
    enum PythonOp {
        SUM, PROD, MAX, MIN, CUSTOM
    };

    enum PythonType {
        INT,
        DOUBLE,
        INT_LIST,
        DOUBLE_LIST
    };

    class PythonFunc {
    public:
        explicit PythonFunc(PythonOp op) : op(op) {}
        PythonFunc(PythonOp op, const boost::python::object& func, bool comm, bool assoc) : op(op), func(func), comm(comm), assoc(assoc) {}

        PythonOp op;
        boost::python::object func;
        bool comm;
        bool assoc;
    };

    class PythonData {
    public:
        explicit PythonData(PythonType type) : type(type) {}
        PythonData(PythonType type, unsigned int num_objects) : type(type), num_objects(num_objects) {}

        PythonType type;
        unsigned int num_objects;
    };

    class PythonBindingSupport {
    protected:
        template<typename T>
        T extract_object(const boost::python::object& py_obj) {
            boost::python::extract<T> object(py_obj);
            if (!object.check()) {
                throw std::runtime_error("Conversion of value not possible");
            }
            return object();
        }

        template<typename T>
        std::vector<T> extract_list(const boost::python::object& py_obj) {
            boost::python::extract<boost::python::list> extracted(py_obj);
            if (!extracted.check()) {
                throw std::runtime_error("Could not convert to list");
            }
            return to_vec<T>(extracted());
        }

        template<typename T>
        std::vector<T> to_vec(const boost::python::list& iterable) {
            return std::vector<T>(
                    boost::python::stl_input_iterator<T>(iterable),
                    boost::python::stl_input_iterator<T>());
        }

        template<typename T>
        boost::python::list to_list(const std::vector<T>& value) {
            boost::python::list list;
            for (const auto& item : value) {
                list.append(item);
            }
            return list;
        }

        template<typename T>
        FMI::Utils::Function<T> get_function(PythonFunc function) {
            FMI::Utils::Function<T> func([](T a, T b) { return a + b; }, true, true);
            if (function.op == PROD) {
                func = FMI::Utils::Function<T>([](T a, T b) { return a * b; }, true, true);
            } else if (function.op == MAX) {
                func = FMI::Utils::Function<T>([](T a, T b) { return std::max(a, b); }, true, true);
            } else if (function.op == MIN) {
                func = FMI::Utils::Function<T>([](T a, T b) { return std::min(a, b); }, true, true);
            } else if (function.op == CUSTOM) {
                func = FMI::Utils::Function<T>(
                        [this, function](T a, T b) {
                            return extract_object<T>(function.func(a, b));
                        },
                        function.comm,
                        function.assoc);
            }
            return func;
        }

        template<typename A>
        FMI::Utils::Function<std::vector<A>> get_vec_function(PythonFunc function) {
            using T = std::vector<A>;
            FMI::Utils::Function<T> func(
                    [](T a, T b) {
                        std::transform(a.begin(), a.end(), b.begin(), a.begin(), std::plus<A>());
                        return a;
                    },
                    true,
                    true);
            if (function.op == PROD) {
                func = FMI::Utils::Function<T>(
                        [](T a, T b) {
                            std::transform(a.begin(), a.end(), b.begin(), a.begin(), std::multiplies<A>());
                            return a;
                        },
                        true,
                        true);
            } else if (function.op == MAX) {
                func = FMI::Utils::Function<T>(
                        [](T a, T b) {
                            std::transform(a.begin(), a.end(), b.begin(), a.begin(), [](A lhs, A rhs) { return std::max(lhs, rhs); });
                            return a;
                        },
                        true,
                        true);
            } else if (function.op == MIN) {
                func = FMI::Utils::Function<T>(
                        [](T a, T b) {
                            std::transform(a.begin(), a.end(), b.begin(), a.begin(), [](A lhs, A rhs) { return std::min(lhs, rhs); });
                            return a;
                        },
                        true,
                        true);
            } else if (function.op == CUSTOM) {
                auto iter = [this, function](A lhs, A rhs) {
                    return extract_object<A>(function.func(lhs, rhs));
                };
                func = FMI::Utils::Function<T>(
                        [iter](T a, T b) {
                            std::transform(a.begin(), a.end(), b.begin(), a.begin(), iter);
                            return a;
                        },
                        function.comm,
                        function.assoc);
            }
            return func;
        }
    };
}

#endif //FMI_PYTHONBINDINGSUPPORT_H
