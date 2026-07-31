// Storage for the shape registry declared in shapes.h.
//
// Same construction as the behavioural corpus registry (tests/migration_counterexample_*):
// function-local statics, so a registrar running during static initialisation can never touch an
// uninitialised container, and a freeze flag that turns a too-late registration into a reported
// error rather than a shape that silently disappears.

#include "shapes.h"

#include <algorithm>
#include <set>

namespace FMI::Runbooks::Checkpoint {
namespace {

struct RegistryEntry {
    const char* name;
    const char* description;
    ShapeRun run;
    //! Registration order, used only to break ties deterministically.
    std::size_t order;
};

std::vector<RegistryEntry>& registry() {
    static std::vector<RegistryEntry> entries;
    return entries;
}

bool& registry_frozen() {
    static bool frozen = false;
    return frozen;
}

std::string& mutable_registry_error() {
    static std::string error;
    return error;
}

std::vector<Shape> build_catalog() {
    registry_frozen() = true;

    auto entries = registry();
    // Deterministic ordering independent of link order: by name, then registration order.
    std::stable_sort(entries.begin(), entries.end(),
                     [](const RegistryEntry& left, const RegistryEntry& right) {
                         const int by_name = std::string(left.name).compare(right.name);
                         if (by_name != 0) return by_name < 0;
                         return left.order < right.order;
                     });

    std::vector<Shape> catalog;
    catalog.reserve(entries.size());
    for (const auto& entry : entries) {
        catalog.push_back({entry.name, entry.description, entry.run});
    }
    return catalog;
}

const std::string& validate_catalog(const std::vector<Shape>& catalog) {
    static const std::string error = [&catalog] {
        if (!mutable_registry_error().empty()) return mutable_registry_error();
        if (catalog.empty()) {
            return std::string{"no shapes are registered: is shapes/ empty, or was a shape "
                               "file left out of the build?"};
        }
        std::set<std::string> names;
        for (const auto& shape : catalog) {
            if (!names.insert(shape.name).second) {
                return std::string{"duplicate shape name: "} + shape.name;
            }
        }
        return std::string{};
    }();
    return error;
}

} // namespace

ShapeRegistrar::ShapeRegistrar(const char* name, const char* description, ShapeRun run) {
    if (name == nullptr || *name == '\0' || run == nullptr) {
        mutable_registry_error() = "a shape translation unit registered with an empty name or "
                                   "a null run function";
        return;
    }
    if (registry_frozen()) {
        mutable_registry_error() =
                std::string("shape registered after the catalog was built: ") + name;
        return;
    }
    registry().push_back({name, description == nullptr ? "" : description, run,
                          registry().size()});
}

const std::vector<Shape>& shapes() {
    static const std::vector<Shape> catalog = build_catalog();
    return catalog;
}

const Shape* find_shape(const std::string& name) {
    for (const auto& shape : shapes()) {
        if (name == shape.name) {
            return &shape;
        }
    }
    return nullptr;
}

const std::string& registry_error() {
    return validate_catalog(shapes());
}

} // namespace FMI::Runbooks::Checkpoint
