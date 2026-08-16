#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <sys/mman.h>

namespace fixed_size_payload {

    inline std::size_t mb_to_bytes(long mb) {
        if (mb <= 0)
            return 0;
        return static_cast<std::size_t>(mb) * 1024ULL * 1024ULL;
    }

    inline void *alloc_and_fill(std::size_t bytes) {
        if (bytes == 0)
            return nullptr;

        void *p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED)
            throw std::runtime_error("fixed_size_payload: mmap failed");

        char *cp = static_cast<char *>(p);
        for (std::size_t i = 0; i < bytes; i++)
            cp[i] = i & 13;

        return p;
    }

    inline void free_payload(void *p, std::size_t bytes) {
        if (p && bytes)
            munmap(p, bytes);
    }

} // namespace fixed_size_payload
