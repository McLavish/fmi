# Initial CRIU experiments

These are July 2026 observations from a small process experiment. They establish
behavior in the tested environment, not a general guarantee for every kernel,
privilege setup, or FMI application. Current integration evidence is in
[fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration/tree/main/benchmarks/migration).

## Environment

The host ran Ubuntu 24.04, kernel `7.0.0-28-generic`, and CRIU 4.2 at
`/usr/local/sbin/criu`. CRIU ran as uid 1000 with `--unprivileged`, but its binary
had these file capabilities:

```
cap_net_admin,cap_sys_ptrace,cap_sys_admin,cap_sys_resource,cap_checkpoint_restore=eip
```

The subject was a roughly 60-line program, independent of FMI, that wrote an
increasing counter to a file. Optional features were a second thread holding a
mutex and a loopback TCP connection with both endpoints in the process.

## Results

| Experiment | Observation |
|---|---|
| Dump and restore as the configured non-root user | Both returned zero with `--unprivileged` |
| Same-host restore while the original remained stopped | Failed because the original still held the PID |
| Restore two threads with a mutex held | Both threads restored and the counter continued |
| Discard an established TCP connection | Required `--tcp-close` on both dump and restore in this mode |
| Repeat dump and restore twice | Counter state continued through both cycles |

### Same-host restore and `--leave-stopped`

```
criu dump --unprivileged -t <pid> --leave-stopped     rc=0
/proc/<pid>/stat state                                 T          (stopped, pid still held)
criu restore --unprivileged                            rc=1
  Error (criu/cr-restore.c:1230): Can't fork for <pid>: File exists
  Error (criu/cr-restore.c:2324): Restoring FAILED.
kill -9 <pid>; criu restore --unprivileged            rc=0        (same pid reclaimed)
```

For this same-host, same-PID setup, the original must be removed before its
replacement can restore. The experiment therefore cannot provide an abort path
that restores the replacement while retaining the original as a fallback. PID
namespaces or remapping were not tested.

### Threads and locks

Dump and restore both succeeded with a second thread holding a `std::mutex`.
The restored process still had two threads and its counter advanced from 79 to
99 during a one-second observation. This verifies preservation of that toy
process's state; it does not verify a library progress or control thread.

### Discarding TCP connections

```
dump, no TCP flag        rc=1  inet: Connected TCP socket, consider using --tcp-established
dump --tcp-close         rc=0
restore, no TCP flag     rc=1  Error (criu/image.c:94): Need to set the --tcp-close options.
restore --tcp-close      rc=0
established connections after restore: 0
```

CRIU recorded the selected `--tcp-close` mode in the image and required it again
at restore. Execution resumed and the counter advanced from 320 to 340, with no
established connections remaining. FMI's replay mechanism must reconstruct data
lost with its connections; this experiment did not test that reconstruction.

### Repeated restore

With both the extra thread and TCP connection enabled, the counter observations
were 39 → 79 → 119 → 139 across two complete dump/restore cycles. The thread count
remained two.

## Limits

- `PR_SET_PTRACER` persistence was not tested: `cap_sys_ptrace` allowed the second
  dump regardless of whether that process setting survived.
- No FMI process or hiredis connection was involved. Later FMI experiments
  exposed a post-restore SIGPIPE failure that this toy could not reveal.
- Dump duration as a function of resident memory was not measured.
- Cross-host transfer, clock preservation, and restore leases were not tested.
