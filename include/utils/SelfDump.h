#pragma once

namespace FMI::Utils {
    //! Write an image of this process that a user-space restorer can resume on another host.
    /*!
     * The unprivileged counterpart of `criu dump`: where a sandbox forbids ptrace, the process
     * checkpoints itself. The calling thread interrupts every other thread with a signal whose
     * handler records the thread's registers, FPU state and signal mask and then parks; the
     * caller then walks its own address space and writes manifest.txt, pages.img and vdso.bin
     * into `dir` in the format deploy/aws-lambda/usrestore consumes (the one pack.py derives
     * from a CRIU image), followed by an empty `done` marker. Everything after the threads are
     * parked uses static buffers and raw system calls, since a parked thread may hold any lock.
     *
     * @return 0 in the process that wrote the image: its other threads stay parked and the
     *         caller is expected to exit; 1 in the restored copy, which resumes here as if the
     *         call had just returned; -1 when the image could not be written (the parked
     *         threads are released, an explanation went to stderr).
     */
    int self_dump(const char* dir);
}
