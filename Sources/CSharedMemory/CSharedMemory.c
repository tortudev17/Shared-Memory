#include "CSharedMemory.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdalign.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define SMR_MAGIC "SMRIPC1"
#define SMR_PAGE_ALIGNMENT 4096u

typedef struct {
    char magic[8];
    uint32_t abi_version;
    uint32_t header_size;
    uint64_t region_size;
    uint64_t control_size;
    uint64_t heap_offset;
    uint64_t heap_size;
    uint64_t boot_id;
    uint64_t reserved0;
    alignas(64) uint64_t init_state;
    uint64_t daemon_pid;
    uint64_t heartbeat_nanoseconds;
    uint64_t reserved1[5];
} SMRHeader;

typedef struct {
    alignas(64) uint64_t state;
    uint64_t request_state;
    uint64_t pid;
    uint64_t generation;
    char name[SMR_MAX_NAME_BYTES + 1];
    uint8_t name_padding[64];
    SMRRequest request;
    SMRResponse response;
    alignas(64) uint64_t event_head;
    uint64_t event_tail;
    uint64_t reserved[6];
    SMREvent events[SMR_EVENT_CAPACITY];
} SMRClientSlot;

static volatile sig_atomic_t smr_termination_requested = 0;

static uint64_t smr_align_up(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1u) & ~(alignment - 1u);
}

static uint64_t smr_slots_offset(void) {
    return smr_align_up((uint64_t)sizeof(SMRHeader), 64u);
}

uint64_t smr_control_size(void) {
    uint64_t raw = smr_slots_offset() + ((uint64_t)SMR_MAX_CLIENTS * sizeof(SMRClientSlot));
    return smr_align_up(raw, SMR_PAGE_ALIGNMENT);
}

uint64_t smr_minimum_region_size(void) {
    return smr_control_size() + (1u << 20);
}

int smr_region_create(const char *name, uint64_t size, SMRMapping *mapping) {
    if (name == NULL || mapping == NULL || size < smr_minimum_region_size()) {
        errno = EINVAL;
        return -1;
    }
    memset(mapping, 0, sizeof(*mapping));
    mapping->fd = -1;
    int fd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, 0600);
    if (fd < 0) {
        return -1;
    }
    if (ftruncate(fd, (off_t)size) != 0) {
        int saved = errno;
        close(fd);
        shm_unlink(name);
        errno = saved;
        return -1;
    }
    void *address = mmap(NULL, (size_t)size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) {
        int saved = errno;
        close(fd);
        shm_unlink(name);
        errno = saved;
        return -1;
    }
    mapping->address = address;
    mapping->size = size;
    mapping->fd = fd;
    return 0;
}

int smr_region_open(const char *name, SMRMapping *mapping) {
    if (name == NULL || mapping == NULL) {
        errno = EINVAL;
        return -1;
    }
    memset(mapping, 0, sizeof(*mapping));
    mapping->fd = -1;
    int fd = shm_open(name, O_RDWR, 0600);
    if (fd < 0) {
        return -1;
    }
    struct stat status;
    if (fstat(fd, &status) != 0 || status.st_size <= 0) {
        int saved = errno;
        close(fd);
        errno = saved == 0 ? EINVAL : saved;
        return -1;
    }
    uint64_t size = (uint64_t)status.st_size;
    void *address = mmap(NULL, (size_t)size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) {
        int saved = errno;
        close(fd);
        errno = saved;
        return -1;
    }
    mapping->address = address;
    mapping->size = size;
    mapping->fd = fd;
    return 0;
}

void smr_mapping_close(SMRMapping *mapping) {
    if (mapping == NULL) {
        return;
    }
    if (mapping->address != NULL && mapping->size > 0) {
        munmap(mapping->address, (size_t)mapping->size);
    }
    if (mapping->fd >= 0) {
        close(mapping->fd);
    }
    mapping->address = NULL;
    mapping->size = 0;
    mapping->fd = -1;
}

int smr_region_unlink(const char *name) {
    return shm_unlink(name);
}

int smr_bootstrap_lock(const char *path) {
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        return -1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return -1;
    }
    return fd;
}

void smr_bootstrap_unlock(int fd) {
    if (fd >= 0) {
        flock(fd, LOCK_UN);
        close(fd);
    }
}

int smr_lock_memory(void *address, uint64_t size) {
    if (address == NULL || size == 0) {
        errno = EINVAL;
        return -1;
    }
    return mlock(address, (size_t)size);
}

int smr_region_initialize(void *address, uint64_t region_size, uint64_t boot_id, int32_t daemon_pid) {
    if (address == NULL || region_size < smr_minimum_region_size()) {
        return 0;
    }
    uint64_t control = smr_control_size();
    memset(address, 0, (size_t)control);
    SMRHeader *header = (SMRHeader *)address;
    __atomic_store_n(&header->init_state, 1u, __ATOMIC_RELAXED);
    memcpy(header->magic, SMR_MAGIC, 8);
    header->abi_version = SMR_ABI_VERSION;
    header->header_size = (uint32_t)sizeof(SMRHeader);
    header->region_size = region_size;
    header->control_size = control;
    header->heap_offset = control;
    header->heap_size = region_size - control;
    header->boot_id = boot_id;
    __atomic_store_n(&header->daemon_pid, (uint64_t)(uint32_t)daemon_pid, __ATOMIC_RELAXED);
    __atomic_store_n(&header->heartbeat_nanoseconds, smr_monotonic_nanoseconds(), __ATOMIC_RELAXED);
    __atomic_store_n(&header->init_state, 2u, __ATOMIC_RELEASE);
    return 1;
}

int smr_region_validate(const void *address, uint64_t mapped_size) {
    if (address == NULL || mapped_size < sizeof(SMRHeader)) {
        return 0;
    }
    const SMRHeader *header = (const SMRHeader *)address;
    if (__atomic_load_n(&header->init_state, __ATOMIC_ACQUIRE) != 2u) {
        return 0;
    }
    if (memcmp(header->magic, SMR_MAGIC, 8) != 0 || header->abi_version != SMR_ABI_VERSION) {
        return 0;
    }
    if (header->region_size != mapped_size || header->control_size != smr_control_size()) {
        return 0;
    }
    if (header->heap_offset < header->control_size || header->heap_offset >= header->region_size) {
        return 0;
    }
    return header->heap_size == header->region_size - header->heap_offset;
}

uint64_t smr_region_size(const void *address) {
    return address == NULL ? 0 : ((const SMRHeader *)address)->region_size;
}

uint64_t smr_heap_offset(const void *address) {
    return address == NULL ? 0 : ((const SMRHeader *)address)->heap_offset;
}

uint64_t smr_heap_size(const void *address) {
    return address == NULL ? 0 : ((const SMRHeader *)address)->heap_size;
}

uint64_t smr_boot_id(const void *address) {
    return address == NULL ? 0 : ((const SMRHeader *)address)->boot_id;
}

int32_t smr_daemon_pid(const void *address) {
    if (address == NULL) return 0;
    return (int32_t)__atomic_load_n(&((const SMRHeader *)address)->daemon_pid, __ATOMIC_ACQUIRE);
}

void smr_set_daemon_pid(void *address, int32_t pid) {
    if (address != NULL) {
        __atomic_store_n(&((SMRHeader *)address)->daemon_pid, (uint64_t)(uint32_t)pid, __ATOMIC_RELEASE);
    }
}

uint64_t smr_daemon_heartbeat(const void *address) {
    if (address == NULL) return 0;
    return __atomic_load_n(&((const SMRHeader *)address)->heartbeat_nanoseconds, __ATOMIC_ACQUIRE);
}

void smr_set_daemon_heartbeat(void *address, uint64_t nanoseconds) {
    if (address != NULL) {
        __atomic_store_n(&((SMRHeader *)address)->heartbeat_nanoseconds, nanoseconds, __ATOMIC_RELEASE);
    }
}

void *smr_client_slot(void *address, uint32_t index) {
    if (address == NULL || index >= SMR_MAX_CLIENTS) return NULL;
    return (uint8_t *)address + smr_slots_offset() + ((uint64_t)index * sizeof(SMRClientSlot));
}

const void *smr_const_client_slot(const void *address, uint32_t index) {
    return smr_client_slot((void *)address, index);
}

uint64_t smr_slot_state(const void *slot) {
    if (slot == NULL) return SMR_SLOT_EMPTY;
    return __atomic_load_n(&((const SMRClientSlot *)slot)->state, __ATOMIC_ACQUIRE);
}

int smr_slot_claim(void *slot) {
    if (slot == NULL) return 0;
    uint64_t expected = SMR_SLOT_EMPTY;
    return __atomic_compare_exchange_n(
        &((SMRClientSlot *)slot)->state,
        &expected,
        SMR_SLOT_CLAIMING,
        0,
        __ATOMIC_ACQ_REL,
        __ATOMIC_ACQUIRE
    );
}

int smr_slot_prepare(void *slot, int32_t pid, uint64_t generation, const char *name) {
    if (slot == NULL || name == NULL || strnlen(name, SMR_MAX_NAME_BYTES + 1u) > SMR_MAX_NAME_BYTES) {
        return 0;
    }
    SMRClientSlot *client = (SMRClientSlot *)slot;
    if (__atomic_load_n(&client->state, __ATOMIC_ACQUIRE) != SMR_SLOT_CLAIMING) {
        return 0;
    }
    client->pid = (uint64_t)(uint32_t)pid;
    client->generation = generation;
    memset(client->name, 0, sizeof(client->name));
    memcpy(client->name, name, strlen(name));
    memset(&client->request, 0, sizeof(client->request));
    memset(&client->response, 0, sizeof(client->response));
    __atomic_store_n(&client->event_head, 0u, __ATOMIC_RELAXED);
    __atomic_store_n(&client->event_tail, 0u, __ATOMIC_RELAXED);
    __atomic_store_n(&client->request_state, SMR_REQUEST_IDLE, __ATOMIC_RELEASE);
    return 1;
}

void smr_slot_activate(void *slot) {
    if (slot != NULL) {
        __atomic_store_n(&((SMRClientSlot *)slot)->state, SMR_SLOT_ACTIVE, __ATOMIC_RELEASE);
    }
}

void smr_slot_reset(void *slot) {
    if (slot == NULL) return;
    SMRClientSlot *client = (SMRClientSlot *)slot;
    __atomic_store_n(&client->request_state, SMR_REQUEST_IDLE, __ATOMIC_RELAXED);
    __atomic_store_n(&client->event_head, 0u, __ATOMIC_RELAXED);
    __atomic_store_n(&client->event_tail, 0u, __ATOMIC_RELAXED);
    client->pid = 0;
    client->generation = 0;
    client->name[0] = '\0';
    __atomic_store_n(&client->state, SMR_SLOT_EMPTY, __ATOMIC_RELEASE);
}

int32_t smr_slot_pid(const void *slot) {
    return slot == NULL ? 0 : (int32_t)((const SMRClientSlot *)slot)->pid;
}

uint64_t smr_slot_generation(const void *slot) {
    return slot == NULL ? 0 : ((const SMRClientSlot *)slot)->generation;
}

const char *smr_slot_name(const void *slot) {
    return slot == NULL ? "" : ((const SMRClientSlot *)slot)->name;
}

static int smr_copy_bounded(char *destination, size_t capacity, const char *source) {
    memset(destination, 0, capacity);
    if (source == NULL) return 1;
    size_t length = strnlen(source, capacity);
    if (length >= capacity) return 0;
    memcpy(destination, source, length);
    return 1;
}

int smr_client_submit(
    void *slot,
    uint64_t sequence,
    uint32_t opcode,
    uint32_t flags,
    uint64_t arg0,
    uint64_t arg1,
    uint64_t arg2,
    uint64_t arg3,
    uint64_t arg4,
    uint64_t arg5,
    const char *path,
    const char *target
) {
    if (slot == NULL) return 0;
    SMRClientSlot *client = (SMRClientSlot *)slot;
    if (__atomic_load_n(&client->request_state, __ATOMIC_ACQUIRE) != SMR_REQUEST_IDLE) return 0;
    SMRRequest request;
    memset(&request, 0, sizeof(request));
    request.sequence = sequence;
    request.opcode = opcode;
    request.flags = flags;
    request.arg0 = arg0;
    request.arg1 = arg1;
    request.arg2 = arg2;
    request.arg3 = arg3;
    request.arg4 = arg4;
    request.arg5 = arg5;
    if (!smr_copy_bounded(request.path, sizeof(request.path), path) ||
        !smr_copy_bounded(request.target, sizeof(request.target), target)) {
        return 0;
    }
    memcpy(&client->request, &request, sizeof(request));
    __atomic_store_n(&client->request_state, SMR_REQUEST_PENDING, __ATOMIC_RELEASE);
    return 1;
}

int smr_client_try_response(const void *slot, uint64_t sequence, SMRResponse *response) {
    if (slot == NULL || response == NULL) return 0;
    const SMRClientSlot *client = (const SMRClientSlot *)slot;
    if (__atomic_load_n(&client->request_state, __ATOMIC_ACQUIRE) != SMR_REQUEST_DONE) return 0;
    memcpy(response, &client->response, sizeof(*response));
    return response->sequence == sequence ? 1 : -1;
}

void smr_client_ack_response(void *slot) {
    if (slot != NULL) {
        __atomic_store_n(&((SMRClientSlot *)slot)->request_state, SMR_REQUEST_IDLE, __ATOMIC_RELEASE);
    }
}

int smr_daemon_take_request(void *slot, SMRRequest *request) {
    if (slot == NULL || request == NULL) return 0;
    SMRClientSlot *client = (SMRClientSlot *)slot;
    uint64_t expected = SMR_REQUEST_PENDING;
    if (!__atomic_compare_exchange_n(
            &client->request_state,
            &expected,
            SMR_REQUEST_PROCESSING,
            0,
            __ATOMIC_ACQUIRE,
            __ATOMIC_RELAXED)) {
        return 0;
    }
    memcpy(request, &client->request, sizeof(*request));
    return 1;
}

void smr_daemon_complete_request(void *slot, const SMRResponse *response) {
    if (slot == NULL || response == NULL) return;
    SMRClientSlot *client = (SMRClientSlot *)slot;
    memcpy(&client->response, response, sizeof(*response));
    __atomic_store_n(&client->request_state, SMR_REQUEST_DONE, __ATOMIC_RELEASE);
}

const char *smr_request_path(const SMRRequest *request) {
    return request == NULL ? "" : request->path;
}

const char *smr_request_target(const SMRRequest *request) {
    return request == NULL ? "" : request->target;
}

int smr_daemon_push_event(void *slot, const SMREvent *event) {
    if (slot == NULL || event == NULL) return 0;
    SMRClientSlot *client = (SMRClientSlot *)slot;
    uint64_t head = __atomic_load_n(&client->event_head, __ATOMIC_RELAXED);
    uint64_t tail = __atomic_load_n(&client->event_tail, __ATOMIC_ACQUIRE);
    if (head - tail >= SMR_EVENT_CAPACITY) return 0;
    client->events[head % SMR_EVENT_CAPACITY] = *event;
    __atomic_store_n(&client->event_head, head + 1u, __ATOMIC_RELEASE);
    return 1;
}

int smr_client_pop_event(void *slot, uint64_t generation, SMREvent *event) {
    if (slot == NULL || event == NULL) return 0;
    SMRClientSlot *client = (SMRClientSlot *)slot;
    uint64_t tail = __atomic_load_n(&client->event_tail, __ATOMIC_RELAXED);
    uint64_t head = __atomic_load_n(&client->event_head, __ATOMIC_ACQUIRE);
    if (tail == head) return 0;
    SMREvent candidate = client->events[tail % SMR_EVENT_CAPACITY];
    __atomic_store_n(&client->event_tail, tail + 1u, __ATOMIC_RELEASE);
    if (candidate.generation != generation) return -1;
    *event = candidate;
    return 1;
}

uint64_t smr_event_count(const void *slot) {
    if (slot == NULL) return 0;
    const SMRClientSlot *client = (const SMRClientSlot *)slot;
    uint64_t head = __atomic_load_n(&client->event_head, __ATOMIC_ACQUIRE);
    uint64_t tail = __atomic_load_n(&client->event_tail, __ATOMIC_ACQUIRE);
    return head - tail;
}

uint64_t smr_monotonic_nanoseconds(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) return 0;
    return ((uint64_t)value.tv_sec * 1000000000u) + (uint64_t)value.tv_nsec;
}

void smr_cpu_relax(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause");
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield");
#else
    __asm__ __volatile__("" ::: "memory");
#endif
}

void smr_sleep_nanoseconds(uint64_t nanoseconds) {
    struct timespec request;
    request.tv_sec = (time_t)(nanoseconds / 1000000000u);
    request.tv_nsec = (long)(nanoseconds % 1000000000u);
    while (nanosleep(&request, &request) != 0 && errno == EINTR) {}
}

int smr_pid_is_alive(int32_t pid) {
    if (pid <= 0) return 0;
    if (kill((pid_t)pid, 0) == 0) return 1;
    return errno == EPERM;
}

int32_t smr_current_pid(void) {
    return (int32_t)getpid();
}

int smr_detach_session(void) {
    return setsid() < 0 ? -1 : 0;
}

static void smr_termination_handler(int signal_number) {
    (void)signal_number;
    smr_termination_requested = 1;
}

void smr_install_termination_handlers(void) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = smr_termination_handler;
    sigemptyset(&action.sa_mask);
    sigaction(SIGTERM, &action, NULL);
    sigaction(SIGINT, &action, NULL);
}

int smr_should_terminate(void) {
    return smr_termination_requested != 0;
}

uint32_t smr_crc32_extend(uint32_t previous_crc, const void *bytes, uint64_t count) {
    const uint8_t *cursor = (const uint8_t *)bytes;
    uint32_t crc = ~previous_crc;
    for (uint64_t index = 0; index < count; ++index) {
        crc ^= cursor[index];
        for (unsigned bit = 0; bit < 8; ++bit) {
            uint32_t mask = (uint32_t)-(int32_t)(crc & 1u);
            crc = (crc >> 1u) ^ (0xedb88320u & mask);
        }
    }
    return ~crc;
}

uint32_t smr_crc32(const void *bytes, uint64_t count) {
    return smr_crc32_extend(0u, bytes, count);
}

int32_t smr_error_code(void) {
    return (int32_t)errno;
}

int smr_bytes_equal(const void *left, const void *right, uint64_t count) {
    if (count == 0) return 1;
    if (left == NULL || right == NULL || count > SIZE_MAX) return 0;
    return memcmp(left, right, (size_t)count) == 0;
}

uint64_t smr_peak_resident_bytes(void) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#if defined(__APPLE__)
    return (uint64_t)usage.ru_maxrss;
#else
    return (uint64_t)usage.ru_maxrss * 1024u;
#endif
}
