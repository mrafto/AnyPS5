#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include "SceTypes.hpp"
#include "prx/libc/include/General.hpp"
#include "prx/libc/include/GuestArena.hpp"
#include "prx/libkernel/File/include/ArchivedFile.hpp"
#include "prx/libkernel/File/include/File.hpp"
#include "prx/libkernel/Aio/include/Aio.hpp"
#include "prx/libkernel/File/include/SharedMemory.hpp"
#include "prx/libkernel/KernelErrors.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {

constexpr std::int32_t AioProcessing = 2;
constexpr std::int32_t AioCompleted = 3;
constexpr std::int32_t AioAborted = 4;
constexpr std::int32_t AioMaxQueues = 512;
constexpr std::int32_t AioRequestNumMax = 128;
constexpr std::int32_t AioIdNumMax = 128;
constexpr std::uint32_t AioWaitAnd = 1;
constexpr std::uint32_t AioWaitOr = 2;
constexpr std::int32_t AioSchedulingWindowMax = 128;
constexpr std::int32_t AioDelayedCountMax = 128;
constexpr std::uint32_t AioSplitSizeMax = 0x1000000;
constexpr std::uint32_t AioSplitChunkSizeMax = 0x1000000;

enum class Phase { Idle, Queued, Running };

struct Job {
    std::int32_t id;
    bool write;
    std::vector<KernelAioRwRequest> requests;
    std::vector<std::int32_t> descriptors;
};

std::mutex g_mutex;
std::condition_variable g_workAvailable;
std::condition_variable g_changed;
std::array<std::int32_t, AioMaxQueues> g_states{};
std::array<bool, AioMaxQueues> g_occupied{};
std::array<Phase, AioMaxQueues> g_phases{};
std::deque<Job> g_queue;
std::vector<std::int32_t> g_activeDescriptors;
std::int32_t g_nextId = 1;
std::size_t g_workers = 0;
std::atomic<Aio::TransferGate> g_gate{nullptr};

bool ValidId(std::int32_t id) {
    return id > 0 && id < AioMaxQueues;
}

bool AllocateIds(std::int32_t count, std::int32_t* ids) {
    std::lock_guard<std::mutex> lock(g_mutex);
    std::int32_t found = 0;
    std::int32_t candidate = g_nextId;
    for (std::int32_t scanned = 0; scanned < AioMaxQueues - 1 && found < count; ++scanned) {
        if (!g_occupied[candidate]) ids[found++] = candidate;
        candidate = candidate + 1 >= AioMaxQueues ? 1 : candidate + 1;
    }
    if (found < count) return false;
    for (std::int32_t i = 0; i < count; ++i) {
        g_occupied[ids[i]] = true;
        g_states[ids[i]] = AioProcessing;
        g_phases[ids[i]] = Phase::Idle;
    }
    g_nextId = candidate;
    return true;
}

void RequireIds(std::int32_t count, std::int32_t* ids, const char* name) {
    if (!AllocateIds(count, ids)) throw std::runtime_error(std::string(name) + ": no free request id is left (a request id stays in use until sceKernelAioDeleteRequest releases it; the console's result when they run out is not known)");
}

bool Uses(const std::vector<std::int32_t>& descriptors, std::int32_t fd) {
    return std::find(descriptors.begin(), descriptors.end(), fd) != descriptors.end();
}

bool Conflicts(const Job& job, const std::vector<std::int32_t>& blocked) {
    return std::any_of(job.descriptors.begin(), job.descriptors.end(), [&blocked](std::int32_t fd) { return Uses(blocked, fd); });
}

bool TakeRunnable(Job& job) {
    std::vector<std::int32_t> blocked = g_activeDescriptors;
    for (auto candidate = g_queue.begin(); candidate != g_queue.end(); ++candidate) {
        if (!Conflicts(*candidate, blocked)) {
            job = std::move(*candidate);
            g_queue.erase(candidate);
            return true;
        }
        blocked.insert(blocked.end(), candidate->descriptors.begin(), candidate->descriptors.end());
    }
    return false;
}

bool DescriptorBusy(std::int32_t fd) {
    if (Uses(g_activeDescriptors, fd)) return true;
    return std::any_of(g_queue.begin(), g_queue.end(), [fd](const Job& job) { return Uses(job.descriptors, fd); });
}

void WaitIdle(std::unique_lock<std::mutex>& lock, std::int32_t id) {
    g_changed.wait(lock, [id] { return g_phases[id] == Phase::Idle; });
}

void ReleaseId(std::int32_t id) {
    std::unique_lock<std::mutex> lock(g_mutex);
    const auto queued = std::find_if(g_queue.begin(), g_queue.end(), [id](const Job& job) { return job.id == id; });
    if (queued != g_queue.end()) {
        g_queue.erase(queued);
        g_phases[id] = Phase::Idle;
        g_states[id] = AioAborted;
    }
    WaitIdle(lock, id);
    g_occupied[id] = false;
    g_states[id] = AioAborted;
    g_changed.notify_all();
}

std::int32_t CancelId(std::int32_t id) {
    std::lock_guard<std::mutex> lock(g_mutex);
    const auto queued = std::find_if(g_queue.begin(), g_queue.end(), [id](const Job& job) { return job.id == id; });
    if (queued != g_queue.end()) {
        for (const KernelAioRwRequest& request : queued->requests) std::atomic_ref<std::uint32_t>(request.result->state).store(static_cast<std::uint32_t>(AioAborted), std::memory_order_release);
        g_queue.erase(queued);
        g_phases[id] = Phase::Idle;
        g_states[id] = AioAborted;
        g_changed.notify_all();
        g_workAvailable.notify_all();
    } else if (g_phases[id] == Phase::Idle && g_occupied[id]) {
        g_states[id] = AioAborted;
        g_changed.notify_all();
    }
    return g_states[id];
}

std::int64_t NativePread(std::int32_t fd, void* buf, std::size_t nbyte, std::int64_t offset) {
    const GuestArena::HostWrite destination(buf, nbyte);
    if (!destination.Open()) {
        errno = EFAULT;
        return -1;
    }
    if (const auto archived = File::ReadArchivedFile(fd, buf, nbyte, &offset)) {
        if (*archived >= 0) return *archived;
        errno = static_cast<int>(-*archived);
        return -1;
    }
#ifdef _WIN32
    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }
    return NativePositioned_nid_no_patch(fd, buf, nbyte, offset, false);
#else
    return static_cast<std::int64_t>(::pread(fd, buf, nbyte, static_cast<off_t>(offset)));
#endif
}

std::int64_t NativePwrite(std::int32_t fd, const void* buf, std::size_t nbyte, std::int64_t offset) {
    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }
    GuestArena::GuestArenaPrepareHostRead_nid_no_patch(buf, nbyte);
#ifdef _WIN32
    return NativePositioned_nid_no_patch(fd, const_cast<void*>(buf), nbyte, offset, true);
#else
    const auto result = File::PwriteSharedMemory_nid_no_patch(fd, buf, nbyte, offset);
    return result == -2 ? static_cast<std::int64_t>(::pwrite(fd, buf, nbyte, static_cast<off_t>(offset))) : result;
#endif
}

void Publish(const KernelAioRwRequest& req, std::int64_t value, std::int32_t state) {
    req.result->return_value = value;
    std::atomic_ref<std::uint32_t>(req.result->state).store(static_cast<std::uint32_t>(state), std::memory_order_release);
}

bool RunRequest(const KernelAioRwRequest& req, bool write) {
    if (const auto gate = g_gate.load(std::memory_order_acquire)) gate(req.fd, write, req.offset);
    const std::int64_t done = write
        ? NativePwrite(req.fd, req.buf, req.nbyte, req.offset)
        : NativePread(req.fd, req.buf, req.nbyte, req.offset);
    if (done < 0) {
        const int error = errno;
        std::int64_t code = static_cast<std::int64_t>(SCE_KERNEL_ERROR_EIO);
        if (error == EBADF) code = static_cast<std::int64_t>(SCE_KERNEL_ERROR_EBADF);
        if (error == EFAULT) code = static_cast<std::int64_t>(SCE_KERNEL_ERROR_EFAULT);
        if (error == EINVAL) code = static_cast<std::int64_t>(SCE_KERNEL_ERROR_EINVAL);
        Publish(req, code, AioAborted);
        return false;
    }
    Publish(req, done, AioCompleted);
    return true;
}

void WorkerLoop() {
    std::unique_lock<std::mutex> lock(g_mutex);
    for (;;) {
        Job job;
        g_workAvailable.wait(lock, [&job] { return TakeRunnable(job); });
        g_phases[job.id] = Phase::Running;
        g_activeDescriptors.insert(g_activeDescriptors.end(), job.descriptors.begin(), job.descriptors.end());
        lock.unlock();
        bool aborted = false;
        for (const KernelAioRwRequest& request : job.requests) {
            if (!RunRequest(request, job.write)) aborted = true;
        }
        lock.lock();
        for (const std::int32_t fd : job.descriptors) g_activeDescriptors.erase(std::find(g_activeDescriptors.begin(), g_activeDescriptors.end(), fd));
        if (g_occupied[job.id]) g_states[job.id] = aborted ? AioAborted : AioCompleted;
        g_phases[job.id] = Phase::Idle;
        g_changed.notify_all();
        g_workAvailable.notify_all();
    }
}

void EnsureWorkers() {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (g_workers != 0) return;
    g_workers = std::clamp<std::size_t>(std::thread::hardware_concurrency() / 2, 2, 4);
    for (std::size_t index = 0; index < g_workers; ++index) std::thread(WorkerLoop).detach();
}

void Enqueue(Job job) {
    EnsureWorkers();
    std::lock_guard<std::mutex> lock(g_mutex);
    for (const KernelAioRwRequest& request : job.requests) {
        if (!Uses(job.descriptors, request.fd)) job.descriptors.push_back(request.fd);
    }
    g_phases[job.id] = Phase::Queued;
    g_queue.push_back(std::move(job));
    g_workAvailable.notify_all();
}

int ValidateRequests(KernelAioRwRequest* req, std::int32_t size, const void* id) {
    if (req == nullptr || id == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (size <= 0) return SCE_KERNEL_ERROR_EINVAL;
    for (std::int32_t i = 0; i < size; ++i) {
        if (req[i].result == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    }
    return 0;
}

int SubmitCommands(KernelAioRwRequest* req, std::int32_t size, bool write, std::int32_t* id, const char* name) {
    const int error = ValidateRequests(req, size, id);
    if (error != 0) return error;
    std::int32_t queue = 0;
    RequireIds(1, &queue, name);
    *id = queue;
    Enqueue(Job{queue, write, std::vector<KernelAioRwRequest>(req, req + size), {}});
    return 0;
}

int SubmitCommandsMultiple(KernelAioRwRequest* req, std::int32_t size, bool write, std::int32_t* id, const char* name) {
    if (req != nullptr && id != nullptr && size > AioRequestNumMax) throw std::runtime_error(std::string(name) + ": more than 128 requests per batch is not modelled");
    const int error = ValidateRequests(req, size, id);
    if (error != 0) return error;
    std::array<std::int32_t, AioRequestNumMax> queues{};
    RequireIds(size, queues.data(), name);
    for (std::int32_t i = 0; i < size; ++i) id[i] = queues[i];
    for (std::int32_t i = 0; i < size; ++i) Enqueue(Job{queues[i], write, {req[i]}, {}});
    return 0;
}

int ValidateIds(const std::int32_t* id, std::int32_t num, const void* out, const char* name) {
    if (id == nullptr || out == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (num < 0) return SCE_KERNEL_ERROR_EINVAL;
    if (num > AioIdNumMax) throw std::runtime_error(std::string(name) + ": more than 128 ids per batch is not modelled");
    for (std::int32_t i = 0; i < num; ++i) {
        if (!ValidId(id[i])) return SCE_KERNEL_ERROR_EINVAL;
    }
    return 0;
}

}

extern "C" {

int APS5_VABI sceKernelAioDeleteRequest(int32_t id, int32_t* ret) {
    if (ret == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (!ValidId(id)) return SCE_KERNEL_ERROR_EINVAL;
    ReleaseId(id);
    *ret = 0;
    return 0;
}

int APS5_VABI sceKernelAioDeleteRequests(int32_t* id, int32_t num, int32_t* ret) {
    const int error = ValidateIds(id, num, ret, "sceKernelAioDeleteRequests");
    if (error != 0) return error;
    for (int32_t i = 0; i < num; ++i) {
        ReleaseId(id[i]);
        ret[i] = 0;
    }
    return 0;
}

int APS5_VABI sceKernelAioCancelRequest(int32_t id, int32_t* state) {
    if (state == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (id == 0) {
        *state = AioProcessing;
        return 0;
    }
    if (!ValidId(id)) return SCE_KERNEL_ERROR_EINVAL;
    *state = CancelId(id);
    return 0;
}

int APS5_VABI sceKernelAioCancelRequests(int32_t* id, int32_t num, int32_t* state) {
    if (id == nullptr || state == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (num < 0) return SCE_KERNEL_ERROR_EINVAL;
    if (num > AioIdNumMax) throw std::runtime_error("sceKernelAioCancelRequests: more than 128 ids per batch is not modelled");
    for (int32_t i = 0; i < num; ++i) {
        if (id[i] != 0 && !ValidId(id[i])) return SCE_KERNEL_ERROR_EINVAL;
    }
    for (int32_t i = 0; i < num; ++i) {
        if (id[i] == 0) {
            state[i] = AioProcessing;
            continue;
        }
        state[i] = CancelId(id[i]);
    }
    return 0;
}

int APS5_VABI sceKernelAioInitializeImpl(void* param, int32_t size) {
    (void)param;
    (void)size;
    return 0;
}

void APS5_VABI sceKernelAioInitializeParam(void* param) {
    if (param == nullptr) throw std::invalid_argument("sceKernelAioInitializeParam: param is null");
}

int APS5_VABI sceKernelAioSetParam(KernelAioSchedulingParam* param, int32_t schedulingWindowSize, int32_t delayedCountLimit, uint32_t enableSplit, uint32_t splitSize, uint32_t splitChunkSize) {
    if (param == nullptr) throw std::invalid_argument("sceKernelAioSetParam: param is null");
    if (schedulingWindowSize < 0 || schedulingWindowSize > AioSchedulingWindowMax) throw std::runtime_error("sceKernelAioSetParam: a scheduling window outside 0..128 is not modelled");
    if (delayedCountLimit < 0 || delayedCountLimit > AioDelayedCountMax) throw std::runtime_error("sceKernelAioSetParam: a delayed count limit outside 0..128 is not modelled");
    if (enableSplit > 1) throw std::runtime_error("sceKernelAioSetParam: a split flag other than 0 or 1 is not modelled");
    if (splitSize > AioSplitSizeMax || splitChunkSize > AioSplitChunkSizeMax) throw std::runtime_error("sceKernelAioSetParam: a split size above 0x1000000 is not modelled");
    param->schedulingWindowSize = schedulingWindowSize;
    param->delayedCountLimit = delayedCountLimit;
    param->enableSplit = enableSplit;
    param->splitSize = splitSize;
    param->splitChunkSize = splitChunkSize;
    return 0;
}

int APS5_VABI sceKernelAioSubmitReadCommands(KernelAioRwRequest* req, int32_t size, int32_t prio, int32_t* id) {
    (void)prio;
    return SubmitCommands(req, size, false, id, "sceKernelAioSubmitReadCommands");
}

int APS5_VABI sceKernelAioSubmitWriteCommands(KernelAioRwRequest* req, int32_t size, int32_t prio, int32_t* id) {
    (void)prio;
    return SubmitCommands(req, size, true, id, "sceKernelAioSubmitWriteCommands");
}

int APS5_VABI sceKernelAioSubmitReadCommandsMultiple(KernelAioRwRequest* req, int32_t size, int32_t prio, int32_t* id) {
    (void)prio;
    return SubmitCommandsMultiple(req, size, false, id, "sceKernelAioSubmitReadCommandsMultiple");
}

int APS5_VABI sceKernelAioSubmitWriteCommandsMultiple(KernelAioRwRequest* req, int32_t size, int32_t prio, int32_t* id) {
    (void)prio;
    return SubmitCommandsMultiple(req, size, true, id, "sceKernelAioSubmitWriteCommandsMultiple");
}

int APS5_VABI sceKernelAioPollRequest(int32_t id, int32_t* state) {
    if (state == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (!ValidId(id)) return SCE_KERNEL_ERROR_EINVAL;
    std::lock_guard<std::mutex> lock(g_mutex);
    *state = g_states[id];
    return 0;
}

int APS5_VABI sceKernelAioWaitRequests(int32_t* id, int32_t num, int32_t* state, uint32_t mode, uint32_t* usec) {
    const int error = ValidateIds(id, num, state, "sceKernelAioWaitRequests");
    if (error != 0) return error;
    if (mode != AioWaitAnd && mode != AioWaitOr) throw std::runtime_error("sceKernelAioWaitRequests: unsupported wait mode");
    const bool timed = usec != nullptr && *usec != 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(timed ? *usec : 0);
    std::unique_lock<std::mutex> lock(g_mutex);
    for (;;) {
        bool allDone = true;
        bool anyCompleted = false;
        for (int32_t i = 0; i < num; ++i) {
            state[i] = g_states[id[i]];
            if (state[i] == AioProcessing) allDone = false;
            if (state[i] == AioCompleted) anyCompleted = true;
        }
        if (allDone || (mode == AioWaitOr && anyCompleted)) return 0;
        if (!timed) {
            g_changed.wait(lock);
        } else if (g_changed.wait_until(lock, deadline) == std::cv_status::timeout) {
            for (int32_t i = 0; i < num; ++i) state[i] = g_states[id[i]];
            return SCE_KERNEL_ERROR_ETIMEDOUT;
        }
    }
}

int APS5_VABI sceKernelAioWaitRequest(int32_t id, int32_t* state, uint32_t* usec) {
    if (state == nullptr) return SCE_KERNEL_ERROR_EFAULT;
    if (!ValidId(id)) return SCE_KERNEL_ERROR_EINVAL;
    const bool timed = usec != nullptr && *usec != 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(timed ? *usec : 0);
    std::unique_lock<std::mutex> lock(g_mutex);
    for (;;) {
        *state = g_states[id];
        if (*state != AioProcessing) return 0;
        if (!timed) {
            g_changed.wait(lock);
        } else if (g_changed.wait_until(lock, deadline) == std::cv_status::timeout) {
            *state = g_states[id];
            return *state == AioProcessing ? SCE_KERNEL_ERROR_ETIMEDOUT : 0;
        }
    }
}

int APS5_VABI sceKernelAioPollRequests(int32_t* id, int32_t num, int32_t* state) {
    const int error = ValidateIds(id, num, state, "sceKernelAioPollRequests");
    if (error != 0) return error;
    std::lock_guard<std::mutex> lock(g_mutex);
    for (int32_t i = 0; i < num; ++i) state[i] = g_states[id[i]];
    return 0;
}

}

namespace Aio {

void DrainDescriptor(std::int32_t fd) {
    std::unique_lock<std::mutex> lock(g_mutex);
    g_changed.wait(lock, [fd] { return !DescriptorBusy(fd); });
}

void SetTransferGate_nid_no_patch(TransferGate gate) {
    g_gate.store(gate, std::memory_order_release);
}

}
