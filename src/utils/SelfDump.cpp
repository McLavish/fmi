#include "../../include/utils/SelfDump.h"

#include <asm/prctl.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <ucontext.h>
#include <unistd.h>

#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>

// The image of a process, written by the process. Layout of the work:
//
//   self_dump()           saves the calling thread's return context, parks every other thread
//                         through the capture signal, then hands over to write_image().
//   capture_handler()     runs in each parked thread: records what usrestore needs to rebuild
//                         the thread (ucontext registers, fxsave area, signal mask, TLS base,
//                         alternate stack, robust list, glibc's tid slot) and sleeps.
//   write_image()         enumerates fds, walks /proc/self/smaps and /proc/self/pagemap, copies
//                         the pages through /proc/self/mem, and emits the manifest.
//
// The restored copy is entered by usrestore through the record of the calling thread, whose
// registers point at resume_stub() on a private stack; the stub rejoins the saved context, so
// self_dump() returns 1 there with the caller's frame exactly as it was at the call.

namespace {
    using u64 = std::uint64_t;

    constexpr int kMaxThreads = 64;
    constexpr int kMaxVmas = 4096;
    constexpr int kMaxPages = 32768;
    constexpr int kMaxFds = 256;
    constexpr int kMaxPipes = 32;
    constexpr unsigned kPipeDataMax = 65536;
    constexpr unsigned kPathMax = 240;
    constexpr u64 kPage = 4096;
    constexpr unsigned kManifestMax = 4u << 20;
    constexpr unsigned kIoBuf = 1u << 20;
    constexpr unsigned kPagemapEntries = 8192;
    constexpr unsigned kMapsMax = 1u << 20;
    //! usrestore links itself at 0x10000000 and keeps its heap below 0x14000000; in a process it
    //! restored those mappings are dead, and the next restorer needs the range for itself.
    constexpr u64 kRestorerLo = 0x10000000UL, kRestorerHi = 0x14000000UL;

    // CRIU's vma status bits, the vocabulary pack.py and usrestore share
    constexpr unsigned kRegular = 1u << 0, kStack = 1u << 1, kVsyscall = 1u << 2, kVdso = 1u << 3,
                       kHeap = 1u << 5, kFilePrivate = 1u << 6, kFileShared = 1u << 7,
                       kAnonShared = 1u << 8, kAnonPrivate = 1u << 9, kVvar = 1u << 12;

    long rsys(long n, long a = 0, long b = 0, long c = 0, long d = 0, long e = 0, long f = 0) {
        long ret;
        register long r10 asm("r10") = d;
        register long r8 asm("r8") = e;
        register long r9 asm("r9") = f;
        asm volatile("syscall"
                     : "=a"(ret)
                     : "a"(n), "D"(a), "S"(b), "d"(c), "r"(r10), "r"(r8), "r"(r9)
                     : "rcx", "r11", "memory");
        return ret;
    }

    struct ThreadRec {
        int tid;
        int have_fx;
        u64 fs_base, clear_tid_addr, sigmask, sas_sp, sas_size, robust_list;
        unsigned sas_flags, robust_len;
        u64 gregs[23];
        unsigned char fx[512] __attribute__((aligned(16)));
    };
    struct Vma {
        u64 start, end, pgoff;
        unsigned prot, flags, status;
        char path[kPathMax];
    };
    struct PageRange {
        u64 vaddr, nr, off;
    };
    struct FdRec {
        int fd, kind, cloexec, end;
        unsigned flags, pipe_id;
        u64 pos;
        char path[kPathMax];
    };
    struct PipeData {
        unsigned pipe_id, len;
        unsigned char data[kPipeDataMax];
    };
    struct SavedContext {
        u64 rbx, rbp, r12, r13, r14, r15, rsp, rip;
    };

    // Static state: nothing below may allocate once the other threads are parked.
    ThreadRec g_recs[kMaxThreads];
    Vma g_vmas[kMaxVmas];
    int g_nvmas;
    PageRange g_pages[kMaxPages];
    int g_npages;
    FdRec g_fds[kMaxFds];
    int g_nfds;
    PipeData g_pipes[kMaxPipes];
    int g_npipes;
    SavedContext g_ctx;
    unsigned char g_resume_stack[65536] __attribute__((aligned(16)));
    char g_manifest[kManifestMax];
    unsigned g_mlen;
    unsigned char g_io[kIoBuf] __attribute__((aligned(4096)));
    u64 g_pagemap[kPagemapEntries];
    char g_maps[kMapsMax];
    volatile int g_claimed, g_parked, g_release;
    //! 0 while the image is being written, 1 in the copy of memory the image carries.
    volatile int g_restored;

    void msg(const char* fmt, ...) {
        char buf[512];
        va_list ap;
        va_start(ap, fmt);
        int n = std::vsnprintf(buf, sizeof buf, fmt, ap);
        va_end(ap);
        if (n < 0) return;
        if (n > (int)sizeof buf - 1) n = (int)sizeof buf - 1;
        rsys(SYS_write, 2, (long)"selfdump: ", 10);
        rsys(SYS_write, 2, (long)buf, n);
        rsys(SYS_write, 2, (long)"\n", 1);
    }

    void mf(const char* fmt, ...) {
        if (g_mlen >= kManifestMax - 1) return;
        va_list ap;
        va_start(ap, fmt);
        int n = std::vsnprintf(g_manifest + g_mlen, kManifestMax - g_mlen, fmt, ap);
        va_end(ap);
        if (n > 0) g_mlen += (unsigned)n < kManifestMax - g_mlen ? (unsigned)n : kManifestMax - g_mlen - 1;
    }

    //! pack.py's percent-encoding: urllib.parse.quote(path, safe="/"), "-" for an empty path.
    void enc(const char* s, char* out, unsigned outlen) {
        unsigned o = 0;
        if (!*s) {
            if (outlen > 1) { out[0] = '-'; out[1] = 0; }
            return;
        }
        for (; *s && o + 4 < outlen; s++) {
            unsigned char c = (unsigned char)*s;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' ||
                c == '-' || c == '~' || c == '/') {
                out[o++] = (char)c;
            } else {
                static const char hex[] = "0123456789ABCDEF";
                out[o++] = '%';
                out[o++] = hex[c >> 4];
                out[o++] = hex[c & 15];
            }
        }
        out[o] = 0;
    }

    // ---- the saved context of the calling thread -------------------------------------------

    extern "C" int fmi_selfdump_save_context(SavedContext*) __attribute__((returns_twice));
    extern "C" [[noreturn]] void fmi_selfdump_restore_context(SavedContext*);
    asm(R"(
    .text
    .globl fmi_selfdump_save_context
    .type fmi_selfdump_save_context,@function
fmi_selfdump_save_context:
    movq %rbx, 0(%rdi)
    movq %rbp, 8(%rdi)
    movq %r12, 16(%rdi)
    movq %r13, 24(%rdi)
    movq %r14, 32(%rdi)
    movq %r15, 40(%rdi)
    leaq 8(%rsp), %rax
    movq %rax, 48(%rdi)
    movq (%rsp), %rax
    movq %rax, 56(%rdi)
    xorl %eax, %eax
    ret
    .size fmi_selfdump_save_context, .-fmi_selfdump_save_context
    .globl fmi_selfdump_restore_context
    .type fmi_selfdump_restore_context,@function
fmi_selfdump_restore_context:
    movq 0(%rdi), %rbx
    movq 8(%rdi), %rbp
    movq 16(%rdi), %r12
    movq 24(%rdi), %r13
    movq 32(%rdi), %r14
    movq 40(%rdi), %r15
    movq 48(%rdi), %rsp
    movl $1, %eax
    jmp *56(%rdi)
    .size fmi_selfdump_restore_context, .-fmi_selfdump_restore_context
    .previous
    )");

    //! Where usrestore enters the calling thread of the restored copy.
    extern "C" [[noreturn]] void fmi_selfdump_resume_stub() {
        fmi_selfdump_restore_context(&g_ctx);
    }

    // ---- the parked threads ------------------------------------------------------------------

    //! glibc's tid word: `struct pthread::tid`, at offset 720 of the TLS base (x86-64, glibc
    //! 2.39). The kernel knows it as the thread's clear_child_tid address and says so through
    //! PR_GET_TID_ADDRESS; where that prctl is filtered, the word holding the kernel's tid is
    //! taken, and in a restored process, whose glibc keeps the tid it was dumped with, the
    //! known offset is trusted if it holds a plausible tid.
    u64 find_tid_slot(u64 fs, int tid) {
        u64 addr = 0;
        if (rsys(SYS_prctl, 40 /* PR_GET_TID_ADDRESS */, (long)&addr, 0, 0, 0) == 0 && addr) return addr;
        if (!fs) return 0;
        if (*(const volatile int*)(fs + 720) == tid) return fs + 720;
        for (u64 off = 0; off < 1024; off += 4)
            if (*(const volatile int*)(fs + off) == tid) return fs + off;
        const int at720 = *(const volatile int*)(fs + 720);
        if (at720 > 0 && at720 <= 4194304) return fs + 720;
        return 0;
    }

    void record_common(ThreadRec* r) {
        u64 fs = 0;
        asm volatile("movq %%fs:0, %0" : "=r"(fs));
        if (!fs) rsys(SYS_arch_prctl, ARCH_GET_FS, (long)&fs);
        r->fs_base = fs;
        stack_t ss;
        std::memset(&ss, 0, sizeof ss);
        rsys(SYS_sigaltstack, 0, (long)&ss);
        r->sas_sp = (u64)ss.ss_sp;
        r->sas_size = ss.ss_size;
        r->sas_flags = (unsigned)ss.ss_flags & ~(unsigned)SS_ONSTACK;
        u64 head = 0;
        std::size_t len = 0;
        rsys(SYS_get_robust_list, 0, (long)&head, (long)&len);
        r->robust_list = head;
        r->robust_len = (unsigned)len;
        r->clear_tid_addr = find_tid_slot(fs, r->tid);
    }

    void capture_handler(int, siginfo_t*, void* ucv) {
        const ucontext_t* uc = static_cast<const ucontext_t*>(ucv);
        const int slot = __sync_fetch_and_add(&g_claimed, 1);
        if (slot < kMaxThreads) {
            ThreadRec* r = &g_recs[slot];
            r->tid = (int)rsys(SYS_gettid);
            for (int i = 0; i < 23; i++) r->gregs[i] = (u64)uc->uc_mcontext.gregs[i];
            if (uc->uc_mcontext.fpregs != nullptr) {
                const unsigned char* s = reinterpret_cast<const unsigned char*>(uc->uc_mcontext.fpregs);
                for (int i = 0; i < 512; i++) r->fx[i] = s[i];
                r->have_fx = 1;
            }
            std::memcpy(&r->sigmask, &uc->uc_sigmask, sizeof r->sigmask);
            record_common(r);
        }
        __sync_fetch_and_add(&g_parked, 1);
        while (!g_release) {
            struct timespec ts = {0, 2000000};
            rsys(SYS_nanosleep, (long)&ts, 0);
        }
    }

    // ---- small /proc helpers (raw syscalls, static buffers) ----------------------------------

    long read_file(const char* path, char* buf, unsigned max) {
        int fd = (int)rsys(SYS_open, (long)path, O_RDONLY | O_CLOEXEC);
        if (fd < 0) return fd;
        unsigned got = 0;
        while (got < max - 1) {
            long r = rsys(SYS_read, fd, (long)(buf + got), max - 1 - got);
            if (r < 0) { rsys(SYS_close, fd); return r; }
            if (r == 0) break;
            got += (unsigned)r;
        }
        rsys(SYS_close, fd);
        buf[got] = 0;
        return got;
    }

    struct linux_dirent64 {
        u64 d_ino;
        std::int64_t d_off;
        unsigned short d_reclen;
        unsigned char d_type;
        char d_name[];
    };

    //! Numeric entries of a /proc directory into `out`; returns the count or a negative errno.
    int list_numeric(const char* path, int* out, int max, int* dirfd_out) {
        int fd = (int)rsys(SYS_open, (long)path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) return fd;
        if (dirfd_out) *dirfd_out = fd;
        int n = 0;
        for (;;) {
            long r = rsys(SYS_getdents64, fd, (long)g_io, sizeof g_io);
            if (r < 0) { rsys(SYS_close, fd); return (int)r; }
            if (r == 0) break;
            for (long off = 0; off < r;) {
                const linux_dirent64* d = reinterpret_cast<const linux_dirent64*>(g_io + off);
                off += d->d_reclen;
                const char* s = d->d_name;
                if (*s < '0' || *s > '9') continue;
                int v = 0;
                for (; *s >= '0' && *s <= '9'; s++) v = v * 10 + (*s - '0');
                if (n < max) out[n++] = v;
            }
        }
        rsys(SYS_close, fd);
        return n;
    }

    //! The `Key:` line of a /proc status file, parsed as hex.
    bool status_hex(const char* text, const char* key, u64* value) {
        const char* p = std::strstr(text, key);
        if (!p) return false;
        p += std::strlen(key);
        while (*p == ' ' || *p == '\t') p++;
        u64 v = 0;
        for (; ; p++) {
            char c = *p;
            int d = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
            if (d < 0) break;
            v = (v << 4) | (u64)d;
        }
        *value = v;
        return true;
    }

    int mkdir_p(const char* dir) {
        char tmp[kPathMax];
        std::snprintf(tmp, sizeof tmp, "%s", dir);
        for (char* p = tmp + 1; *p; p++) {
            if (*p != '/') continue;
            *p = 0;
            long r = rsys(SYS_mkdir, (long)tmp, 0755);
            if (r < 0 && r != -EEXIST) return (int)r;
            *p = '/';
        }
        long r = rsys(SYS_mkdir, (long)tmp, 0755);
        return r < 0 && r != -EEXIST ? (int)r : 0;
    }

    int open_out(const char* dir, const char* name) {
        char path[kPathMax + 32];
        std::snprintf(path, sizeof path, "%s/%s", dir, name);
        return (int)rsys(SYS_open, (long)path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    }

    long write_all(int fd, const void* buf, u64 len) {
        u64 done = 0;
        while (done < len) {
            long r = rsys(SYS_write, fd, (long)((const char*)buf + done), len - done);
            if (r <= 0) return r < 0 ? r : -EIO;
            done += (u64)r;
        }
        return (long)done;
    }

    // ---- fds ---------------------------------------------------------------------------------

    int collect_fds() {
        int nums[kMaxFds + 8];
        int dirfd = -1;
        int n = list_numeric("/proc/self/fd", nums, kMaxFds + 8, &dirfd);
        if (n < 0) { msg("cannot list /proc/self/fd: %d", n); return -1; }
        g_nfds = 0;
        g_npipes = 0;
        for (int i = 0; i < n; i++) {
            int fd = nums[i];
            if (fd == dirfd) continue;
            char link[64], target[kPathMax];
            std::snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
            long l = rsys(SYS_readlink, (long)link, (long)target, sizeof target - 1);
            if (l < 0) { msg("readlink fd %d: %ld", fd, l); return -1; }
            target[l] = 0;
            struct stat st;
            if (rsys(SYS_fstat, fd, (long)&st) < 0) { msg("fstat fd %d", fd); return -1; }
            if (g_nfds >= kMaxFds) { msg("too many fds"); return -1; }
            FdRec* f = &g_fds[g_nfds];
            std::memset(f, 0, sizeof *f);
            f->fd = fd;
            f->flags = (unsigned)rsys(SYS_fcntl, fd, F_GETFL);
            f->cloexec = (rsys(SYS_fcntl, fd, F_GETFD) & FD_CLOEXEC) ? 1 : 0;
            if (S_ISFIFO(st.st_mode) && !std::strncmp(target, "pipe:[", 6)) {
                f->kind = 2;
                f->pipe_id = (unsigned)st.st_ino;
                f->end = (f->flags & O_ACCMODE) == O_WRONLY ? 1 : 0;
                if (!f->end) {
                    int k;
                    for (k = 0; k < g_npipes; k++) if (g_pipes[k].pipe_id == f->pipe_id) break;
                    if (k == g_npipes) {
                        if (k >= kMaxPipes) { msg("too many pipes"); return -1; }
                        g_pipes[k].pipe_id = f->pipe_id;
                        g_pipes[k].len = 0;
                        int avail = 0;
                        if (rsys(SYS_ioctl, fd, FIONREAD, (long)&avail) == 0 && avail > 0) {
                            if ((unsigned)avail > kPipeDataMax) { msg("pipe %u holds %d bytes, too many", f->pipe_id, avail); return -1; }
                            long r = rsys(SYS_read, fd, (long)g_pipes[k].data, avail);
                            if (r < 0) { msg("read pipe %u: %ld", f->pipe_id, r); return -1; }
                            g_pipes[k].len = (unsigned)r;
                        }
                        g_npipes++;
                    }
                }
            } else if (S_ISREG(st.st_mode) || S_ISCHR(st.st_mode)) {
                f->kind = 1;
                f->pos = (u64)rsys(SYS_lseek, fd, 0, SEEK_CUR);
                if ((long)f->pos < 0) f->pos = 0;
                if (std::strstr(target, " (deleted)")) { msg("fd %d is %s: unlinked files cannot be reopened", fd, target); return -1; }
                std::snprintf(f->path, sizeof f->path, "%s", target);
            } else {
                msg("fd %d is %s: only regular files and pipes can be carried (drain first)", fd, target);
                return -1;
            }
            g_nfds++;
        }
        return 0;
    }

    // ---- the address space -------------------------------------------------------------------

    //! /proc/self/smaps into g_vmas. smaps rather than maps because only its VmFlags line says
    //! whether a mapping grows down, which the restored copy's main stack must keep doing.
    int collect_vmas() {
        long n = read_file("/proc/self/smaps", g_maps, kMapsMax);
        bool have_flags = n > 0;
        if (n <= 0) n = read_file("/proc/self/maps", g_maps, kMapsMax);
        if (n <= 0) { msg("cannot read /proc/self/maps: %ld", n); return -1; }
        if (n >= (long)kMapsMax - 1) { msg("address map larger than %u bytes", kMapsMax); return -1; }
        g_nvmas = 0;
        Vma* cur = nullptr;
        char* line = g_maps;
        while (line && *line) {
            char* nl = std::strchr(line, '\n');
            if (nl) *nl = 0;
            // a header line: "start-end perms offset dev inode [path]"
            char* p = line;
            u64 start = 0, end = 0;
            bool header = false;
            {
                char* q = p;
                while ((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f')) { start = (start << 4) | (u64)(*q <= '9' ? *q - '0' : *q - 'a' + 10); q++; }
                if (*q == '-' && q > p) {
                    q++;
                    char* r = q;
                    while ((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f')) { end = (end << 4) | (u64)(*q <= '9' ? *q - '0' : *q - 'a' + 10); q++; }
                    if (*q == ' ' && q > r) { header = true; p = q + 1; }
                }
            }
            if (header) {
                if (g_nvmas >= kMaxVmas) { msg("too many vmas"); return -1; }
                cur = &g_vmas[g_nvmas++];
                std::memset(cur, 0, sizeof *cur);
                cur->start = start;
                cur->end = end;
                const char* perms = p;
                cur->prot = (perms[0] == 'r' ? PROT_READ : 0) | (perms[1] == 'w' ? PROT_WRITE : 0) | (perms[2] == 'x' ? PROT_EXEC : 0);
                const bool shared = perms[3] == 's';
                p += 5;
                u64 off = 0;
                while ((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')) { off = (off << 4) | (u64)(*p <= '9' ? *p - '0' : *p - 'a' + 10); p++; }
                cur->pgoff = off;
                p++;                                   // dev
                while (*p && *p != ' ') p++;
                p++;                                   // inode
                u64 inode = 0;
                while (*p >= '0' && *p <= '9') { inode = inode * 10 + (u64)(*p - '0'); p++; }
                while (*p == ' ') p++;
                const char* path = p;
                cur->flags = shared ? MAP_SHARED : MAP_PRIVATE;
                if (!*path) {
                    cur->status = kRegular | (shared ? kAnonShared : kAnonPrivate);
                    cur->flags |= MAP_ANONYMOUS;
                } else if (!std::strcmp(path, "[vdso]")) {
                    cur->status = kRegular | kVdso;
                } else if (!std::strcmp(path, "[vvar]") || !std::strcmp(path, "[vvar_vclock]")) {
                    cur->status = kRegular | kVvar;
                } else if (!std::strcmp(path, "[vsyscall]")) {
                    cur->status = kVsyscall;
                } else if (path[0] == '[') {
                    cur->status = kRegular | (shared ? kAnonShared : kAnonPrivate);
                    cur->flags |= MAP_ANONYMOUS;
                    if (!std::strcmp(path, "[heap]")) cur->status |= kHeap;
                    if (!std::strcmp(path, "[stack]")) { cur->status |= kStack; cur->flags |= MAP_GROWSDOWN; }
                } else if (inode == 0 || std::strstr(path, " (deleted)") || !std::strncmp(path, "/dev/", 5) || !std::strncmp(path, "/memfd:", 7)) {
                    msg("mapping %lx-%lx of %s cannot be re-created from a file", start, end, path);
                    return -1;
                } else {
                    cur->status = kRegular | (shared ? kFileShared : kFilePrivate);
                    if (std::strlen(path) >= kPathMax) { msg("path too long: %s", path); return -1; }
                    std::snprintf(cur->path, sizeof cur->path, "%s", path);
                }
            } else if (cur && have_flags && !std::strncmp(line, "VmFlags:", 8)) {
                if (std::strstr(line, " gd")) {
                    cur->flags |= MAP_GROWSDOWN;
                    if (!(cur->status & (kVdso | kVvar | kVsyscall))) cur->status |= kStack;
                }
            }
            line = nl ? nl + 1 : nullptr;
        }
        return g_nvmas ? 0 : -1;
    }

    bool skip_vma(const Vma* v) {
        return v->start >= kRestorerLo && v->end <= kRestorerHi;
    }

    //! Append `len` bytes of own memory at `addr` to pages.img, one record per run of pages
    //! that are not entirely zero when `drop_zero` is set: an unprivileged pagemap cannot tell
    //! the shared zero page from a written one, and a restored anonymous page reads as zero
    //! anyway. (A private file mapping's copy-on-write page must be kept even when zero: the
    //! file underneath may say otherwise.)
    int flush_run(int pages_fd, int mem_fd, u64 addr, u64 len, bool drop_zero, u64* off) {
        for (u64 done = 0; done < len;) {
            u64 chunk = len - done;
            if (chunk > kIoBuf) chunk = kIoBuf;
            long got = rsys(SYS_pread64, mem_fd, (long)g_io, chunk, addr + done);
            if (got != (long)chunk) { msg("read own memory at %lx: %ld", addr + done, got); return -1; }
            u64 keep_from = 0, keep_len = 0;
            for (u64 p = 0; p < chunk; p += kPage) {
                bool zero = false;
                if (drop_zero) {
                    zero = true;
                    const u64* w = reinterpret_cast<const u64*>(g_io + p);
                    for (unsigned i = 0; i < kPage / 8; i++) if (w[i]) { zero = false; break; }
                }
                if (!zero) {
                    if (keep_len && keep_from + keep_len == p) { keep_len += kPage; continue; }
                }
                if (keep_len) {
                    if (write_all(pages_fd, g_io + keep_from, keep_len) < 0) { msg("write pages.img"); return -1; }
                    const u64 vaddr = addr + done + keep_from;
                    if (g_npages && g_pages[g_npages - 1].vaddr + g_pages[g_npages - 1].nr * kPage == vaddr) {
                        g_pages[g_npages - 1].nr += keep_len / kPage;
                    } else {
                        if (g_npages >= kMaxPages) { msg("too many page ranges"); return -1; }
                        g_pages[g_npages++] = PageRange{vaddr, keep_len / kPage, *off};
                    }
                    *off += keep_len;
                    keep_len = 0;
                }
                if (!zero) { keep_from = p; keep_len = kPage; }
            }
            if (keep_len) {
                if (write_all(pages_fd, g_io + keep_from, keep_len) < 0) { msg("write pages.img"); return -1; }
                const u64 vaddr = addr + done + keep_from;
                if (g_npages && g_pages[g_npages - 1].vaddr + g_pages[g_npages - 1].nr * kPage == vaddr) {
                    g_pages[g_npages - 1].nr += keep_len / kPage;
                } else {
                    if (g_npages >= kMaxPages) { msg("too many page ranges"); return -1; }
                    g_pages[g_npages++] = PageRange{vaddr, keep_len / kPage, *off};
                }
                *off += keep_len;
            }
            done += chunk;
        }
        return 0;
    }

    //! Which pages the image must carry: present or swapped, and either not backed by a file
    //! page (anonymous, or a private file mapping's copy-on-write page) or in shared anonymous
    //! memory. A shared file mapping keeps its content in the file.
    int dump_pages(int pages_fd, int pagemap_fd, int mem_fd, u64* total) {
        u64 off = 0;
        g_npages = 0;
        for (int i = 0; i < g_nvmas; i++) {
            const Vma* v = &g_vmas[i];
            if (skip_vma(v) || (v->status & (kVdso | kVvar | kVsyscall)) || (v->status & kFileShared)) continue;
            const bool file_private = (v->status & kFilePrivate) != 0;
            u64 run_start = 0, run_len = 0;
            for (u64 addr = v->start; addr < v->end;) {
                u64 chunk = (v->end - addr) / kPage;
                if (chunk > kPagemapEntries) chunk = kPagemapEntries;
                long r = rsys(SYS_pread64, pagemap_fd, (long)g_pagemap, chunk * 8, (addr / kPage) * 8);
                if (r != (long)(chunk * 8)) { msg("pagemap read at %lx: %ld", addr, r); return -1; }
                for (u64 k = 0; k < chunk; k++) {
                    const u64 e = g_pagemap[k];
                    const bool present = (e >> 63) & 1, swapped = (e >> 62) & 1, file_or_shared = (e >> 61) & 1;
                    const bool want = (present || swapped) && !(file_private && file_or_shared);
                    const u64 page = addr + k * kPage;
                    if (want && run_len && page == run_start + run_len * kPage) { run_len++; continue; }
                    if (run_len) {
                        if (flush_run(pages_fd, mem_fd, run_start, run_len * kPage, !file_private, &off) < 0) return -1;
                        run_len = 0;
                    }
                    if (want) { run_start = page; run_len = 1; }
                }
                addr += chunk * kPage;
            }
            if (run_len && flush_run(pages_fd, mem_fd, run_start, run_len * kPage, !file_private, &off) < 0) return -1;
        }
        *total = off;
        return 0;
    }

    // ---- the manifest ------------------------------------------------------------------------

    void emit_thread(const ThreadRec* r, const char* comm) {
        char c[kPathMax];
        enc(comm, c, sizeof c);
        mf("thread %d 0x%lx 0x%lx 0x%lx 0x%lx %lu %u 0x%lx %u %s\n", r->tid, r->fs_base, r->clear_tid_addr, r->sigmask,
           r->sas_sp, r->sas_size, r->sas_flags, r->robust_list, r->robust_len, c);
        const u64* g = r->gregs;
        const u64 csgsfs = g[REG_CSGSFS];
        u64 ss = (csgsfs >> 48) & 0xffff;
        if (!ss) ss = 0x2b;
        // r15 r14 r13 r12 bp bx r11 r10 r9 r8 ax cx dx si di orig_ax ip cs flags sp ss fs_base gs_base ds es fs gs
        mf("regs %d 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx 0x%lx\n",
           r->tid, g[REG_R15], g[REG_R14], g[REG_R13], g[REG_R12], g[REG_RBP], g[REG_RBX], g[REG_R11], g[REG_R10],
           g[REG_R9], g[REG_R8], g[REG_RAX], g[REG_RCX], g[REG_RDX], g[REG_RSI], g[REG_RDI], ~0UL, g[REG_RIP],
           csgsfs & 0xffff, g[REG_EFL], g[REG_RSP], ss, r->fs_base, 0UL, 0UL, 0UL, (csgsfs >> 32) & 0xffff,
           (csgsfs >> 16) & 0xffff);
        mf("fx %d ", r->tid);
        static const char hex[] = "0123456789abcdef";
        char line[1025];
        for (int i = 0; i < 512; i++) { line[2 * i] = hex[r->fx[i] >> 4]; line[2 * i + 1] = hex[r->fx[i] & 15]; }
        line[1024] = 0;
        mf("%s\n", line);
    }

    int write_image(const char* dir, int pid, int nthreads) {
        int pages_fd = -1, vdso_fd = -1, manifest_fd = -1, pagemap_fd = -1, mem_fd = -1;
        int rc = -1;
        u64 pages_total = 0;
        char encoded[kPathMax * 3];
        char text[4096];

        if (collect_fds() < 0) goto out;
        pages_fd = open_out(dir, "pages.img");
        vdso_fd = open_out(dir, "vdso.bin");
        manifest_fd = open_out(dir, "manifest.txt");
        pagemap_fd = (int)rsys(SYS_open, (long)"/proc/self/pagemap", O_RDONLY | O_CLOEXEC);
        mem_fd = (int)rsys(SYS_open, (long)"/proc/self/mem", O_RDONLY | O_CLOEXEC);
        if (pages_fd < 0 || vdso_fd < 0 || manifest_fd < 0) { msg("cannot create the image files in %s", dir); goto out; }
        if (pagemap_fd < 0 || mem_fd < 0) { msg("cannot open /proc/self/pagemap (%d) or /proc/self/mem (%d)", pagemap_fd, mem_fd); goto out; }
        if (collect_vmas() < 0) goto out;
        if (dump_pages(pages_fd, pagemap_fd, mem_fd, &pages_total) < 0) goto out;

        g_mlen = 0;
        mf("manifest 1\npid %d\nthreads %d\n", pid, nthreads);
        {
            long n = read_file("/proc/self/comm", text, sizeof text);
            if (n > 0 && text[n - 1] == '\n') text[n - 1] = 0;
            enc(n > 0 ? text : "", encoded, sizeof encoded);
            mf("comm %s\n", encoded);
        }
        mf("personality %ld\n", rsys(SYS_personality, 0xffffffff));
        mf("mm 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0\n");
        {
            long n = rsys(SYS_readlink, (long)"/proc/self/cwd", (long)text, sizeof text - 1);
            text[n > 0 ? n : 0] = 0;
            enc(text, encoded, sizeof encoded);
            mf("cwd %s\n", encoded);
            long um = rsys(SYS_umask, 0);
            rsys(SYS_umask, um);
            mf("umask %ld\n", um);
        }
        for (int i = 0; i < g_nvmas; i++) {
            const Vma* v = &g_vmas[i];
            if (skip_vma(v)) continue;
            enc(v->path, encoded, sizeof encoded);
            mf("vma 0x%lx 0x%lx %u %u %u %lu %s\n", v->start, v->end, v->prot, v->flags, v->status, v->pgoff, encoded);
            if (v->status & kVdso) {
                mf("vdso_old 0x%lx 0x%lx\n", v->start, v->end);
                for (u64 done = 0; done < v->end - v->start;) {
                    u64 len = v->end - v->start - done;
                    if (len > kIoBuf) len = kIoBuf;
                    long got = rsys(SYS_pread64, mem_fd, (long)g_io, len, v->start + done);
                    if (got != (long)len) { msg("read the vdso: %ld", got); goto out; }
                    if (write_all(vdso_fd, g_io, len) < 0) { msg("write vdso.bin"); goto out; }
                    done += len;
                }
            }
            if ((v->status & kVvar) && !std::strcmp(v->path, "")) {
                // the first vvar-class mapping is the one the restorer is told about
                static bool told = false;
                if (!told) { mf("vvar_old 0x%lx 0x%lx\n", v->start, v->end); told = true; }
            }
        }
        for (int i = 0; i < g_npages; i++) mf("page 0x%lx %lu %lu\n", g_pages[i].vaddr, g_pages[i].nr, g_pages[i].off);
        mf("pages_size %lu\n", pages_total);
        for (int i = 0; i < g_nfds; i++) {
            const FdRec* f = &g_fds[i];
            if (f->kind == 1) {
                enc(f->path, encoded, sizeof encoded);
                mf("fd %d REG %u %lu %s %d\n", f->fd, f->flags, f->pos, encoded, f->cloexec);
            } else {
                mf("fd %d PIPE %u %d %u %d\n", f->fd, f->pipe_id, f->end, f->flags, f->cloexec);
            }
        }
        for (int k = 0; k < g_npipes; k++) {
            mf("pipedata %u ", g_pipes[k].pipe_id);
            if (!g_pipes[k].len) mf("-");
            static const char hex[] = "0123456789abcdef";
            for (unsigned i = 0; i < g_pipes[k].len; i++) {
                char h[3] = {hex[g_pipes[k].data[i] >> 4], hex[g_pipes[k].data[i] & 15], 0};
                mf("%s", h);
            }
            mf("\n");
        }
        for (int sig = 1; sig <= 64; sig++) {
            if (sig == SIGKILL || sig == SIGSTOP) continue;
            struct { u64 handler, flags, restorer, mask; } ksa = {0, 0, 0, 0};
            rsys(SYS_rt_sigaction, sig, 0, (long)&ksa, 8);
            mf("sigaction %d 0x%lx 0x%lx 0x%lx 0x%lx\n", sig, ksa.handler, ksa.flags, ksa.restorer, ksa.mask);
        }
        // the main thread first, as pack.py lists them
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < nthreads; i++) {
                const ThreadRec* r = &g_recs[i];
                if ((r->tid == pid) != (pass == 0)) continue;
                char path[64];
                std::snprintf(path, sizeof path, "/proc/self/task/%d/comm", r->tid);
                long n = read_file(path, text, 32);
                if (n > 0 && text[n - 1] == '\n') text[n - 1] = 0;
                emit_thread(r, n > 0 ? text : "");
            }
        }
        if (g_mlen >= kManifestMax - 1) { msg("manifest too large"); goto out; }
        if (write_all(manifest_fd, g_manifest, g_mlen) < 0) { msg("write manifest.txt"); goto out; }
        rc = 0;
    out:
        if (pages_fd >= 0) rsys(SYS_close, pages_fd);
        if (vdso_fd >= 0) rsys(SYS_close, vdso_fd);
        if (manifest_fd >= 0) rsys(SYS_close, manifest_fd);
        if (pagemap_fd >= 0) rsys(SYS_close, pagemap_fd);
        if (mem_fd >= 0) rsys(SYS_close, mem_fd);
        if (rc == 0) {
            int done = open_out(dir, "done");
            if (done < 0) { msg("cannot write the done marker"); rc = -1; }
            else rsys(SYS_close, done);
        }
        return rc;
    }

    int park_and_write(const char* dir) {
        const int pid = (int)rsys(SYS_getpid);
        const int self = (int)rsys(SYS_gettid);
        const int sig = SIGRTMIN + 6;
        char text[4096];

        if (mkdir_p(dir) < 0) { msg("cannot create %s", dir); return -1; }
        int tids[kMaxThreads + 8];
        int n = list_numeric("/proc/self/task", tids, kMaxThreads + 8, nullptr);
        if (n < 0) { msg("cannot list /proc/self/task: %d", n); return -1; }
        if (n > kMaxThreads) { msg("%d threads, more than %d", n, kMaxThreads); return -1; }
        for (int i = 0; i < n; i++) {
            if (tids[i] == self) continue;
            char path[64];
            std::snprintf(path, sizeof path, "/proc/self/task/%d/status", tids[i]);
            u64 blocked = 0;
            if (read_file(path, text, sizeof text) > 0 && status_hex(text, "SigBlk:", &blocked) && ((blocked >> (sig - 1)) & 1)) {
                msg("thread %d blocks signal %d, which the capture needs", tids[i], sig);
                return -1;
            }
        }

        g_claimed = g_parked = g_release = 0;
        std::memset(g_recs, 0, sizeof g_recs);
        struct sigaction sa;
        std::memset(&sa, 0, sizeof sa);
        sa.sa_sigaction = capture_handler;
        sa.sa_flags = SA_SIGINFO | SA_RESTART;
        if (sigaction(sig, &sa, nullptr) != 0) { msg("cannot install the capture handler"); return -1; }

        int others = 0;
        for (int i = 0; i < n; i++) {
            if (tids[i] == self) continue;
            long r = rsys(SYS_tgkill, pid, tids[i], sig);
            if (r < 0) { msg("tgkill %d: %ld", tids[i], r); g_release = 1; return -1; }
            others++;
        }
        for (int spins = 0; g_parked < others; spins++) {
            struct timespec ts = {0, 1000000};
            rsys(SYS_nanosleep, (long)&ts, 0);
            if (spins > 10000) { msg("only %d of %d threads parked", g_parked, others); g_release = 1; return -1; }
        }

        // The calling thread's own record: the restorer enters it on a private stack at
        // resume_stub, which rejoins the context saved on entry to self_dump.
        ThreadRec* r = &g_recs[others];
        std::memset(r, 0, sizeof *r);
        r->tid = self;
        asm volatile("fxsave64 %0" : "=m"(*r->fx));
        r->have_fx = 1;
        rsys(SYS_rt_sigprocmask, SIG_BLOCK, 0, (long)&r->sigmask, 8);
        record_common(r);
        r->gregs[REG_RIP] = (u64)&fmi_selfdump_resume_stub;
        r->gregs[REG_RSP] = (u64)(g_resume_stack + sizeof g_resume_stack - 8);
        r->gregs[REG_EFL] = 0x202;
        r->gregs[REG_CSGSFS] = 0x33 | (0x2bUL << 48);
        const int nthreads = others + 1;
        for (int i = 0; i < nthreads; i++) {
            if (!g_recs[i].have_fx || !g_recs[i].fs_base || !g_recs[i].clear_tid_addr) {
                msg("thread %d: incomplete record (fx %d, fs %lx, tid slot %lx)", g_recs[i].tid, g_recs[i].have_fx, g_recs[i].fs_base, g_recs[i].clear_tid_addr);
                g_release = 1;
                return -1;
            }
        }

        g_restored = 1;            // the memory about to be copied belongs to the restored copy
        __sync_synchronize();
        int rc = write_image(dir, pid, nthreads);
        g_restored = 0;
        if (rc < 0) g_release = 1;
        else msg("image of pid %d written to %s: %d threads, %d vmas, %d page ranges, %d fds", pid, dir, nthreads, g_nvmas, g_npages, g_nfds);
        return rc;
    }
}

int FMI::Utils::self_dump(const char* dir) {
    if (fmi_selfdump_save_context(&g_ctx)) {
        return 1;
    }
    return park_and_write(dir);
}
