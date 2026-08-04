#ifndef C_SHARED_MEMORY_H
#define C_SHARED_MEMORY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SMR_ABI_VERSION 1u
#define SMR_MAX_CLIENTS 64u
#define SMR_EVENT_CAPACITY 256u
#define SMR_MAX_NAME_BYTES 127u
#define SMR_MAX_PATH_BYTES 1023u
#define SMR_MAX_TARGET_BYTES 127u

enum {
    SMR_SLOT_EMPTY = 0,
    SMR_SLOT_CLAIMING = 1,
    SMR_SLOT_ACTIVE = 2
};

enum {
    SMR_REQUEST_IDLE = 0,
    SMR_REQUEST_PENDING = 1,
    SMR_REQUEST_PROCESSING = 2,
    SMR_REQUEST_DONE = 3
};

typedef struct {
    uint64_t sequence;
    uint32_t opcode;
    uint32_t flags;
    uint64_t arg0;
    uint64_t arg1;
    uint64_t arg2;
    uint64_t arg3;
    uint64_t arg4;
    uint64_t arg5;
    char path[SMR_MAX_PATH_BYTES + 1];
    char target[SMR_MAX_TARGET_BYTES + 1];
} SMRRequest;

typedef struct {
    uint64_t sequence;
    int64_t status;
    uint64_t value0;
    uint64_t value1;
    uint64_t value2;
    uint64_t value3;
} SMRResponse;

typedef struct {
    uint64_t kind;
    uint64_t generation;
    uint64_t allocation_offset;
    uint64_t payload_offset;
    uint64_t payload_size;
    uint64_t uuid_high;
    uint64_t uuid_low;
    uint64_t value;
} SMREvent;

typedef struct {
    void *address;
    uint64_t size;
    int32_t fd;
    int32_t reserved;
} SMRMapping;

uint64_t smr_control_size(void);
uint64_t smr_minimum_region_size(void);

int smr_region_create(const char *name, uint64_t size, SMRMapping *mapping);
int smr_region_open(const char *name, SMRMapping *mapping);
void smr_mapping_close(SMRMapping *mapping);
int smr_region_unlink(const char *name);

int smr_bootstrap_lock(const char *path);
void smr_bootstrap_unlock(int fd);
int smr_lock_memory(void *address, uint64_t size);

int smr_region_initialize(
    void *address,
    uint64_t region_size,
    uint64_t boot_id,
    int32_t daemon_pid
);
int smr_region_validate(const void *address, uint64_t mapped_size);
uint64_t smr_region_size(const void *address);
uint64_t smr_heap_offset(const void *address);
uint64_t smr_heap_size(const void *address);
uint64_t smr_boot_id(const void *address);
int32_t smr_daemon_pid(const void *address);
void smr_set_daemon_pid(void *address, int32_t pid);
uint64_t smr_daemon_heartbeat(const void *address);
void smr_set_daemon_heartbeat(void *address, uint64_t nanoseconds);

void *smr_client_slot(void *address, uint32_t index);
const void *smr_const_client_slot(const void *address, uint32_t index);
uint64_t smr_slot_state(const void *slot);
int smr_slot_claim(void *slot);
int smr_slot_prepare(void *slot, int32_t pid, uint64_t generation, const char *name);
void smr_slot_activate(void *slot);
void smr_slot_reset(void *slot);
int32_t smr_slot_pid(const void *slot);
uint64_t smr_slot_generation(const void *slot);
const char *smr_slot_name(const void *slot);

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
);
int smr_client_try_response(const void *slot, uint64_t sequence, SMRResponse *response);
void smr_client_ack_response(void *slot);
int smr_daemon_take_request(void *slot, SMRRequest *request);
void smr_daemon_complete_request(void *slot, const SMRResponse *response);
const char *smr_request_path(const SMRRequest *request);
const char *smr_request_target(const SMRRequest *request);

int smr_daemon_push_event(void *slot, const SMREvent *event);
int smr_client_pop_event(void *slot, uint64_t generation, SMREvent *event);
uint64_t smr_event_count(const void *slot);

uint64_t smr_monotonic_nanoseconds(void);
void smr_cpu_relax(void);
void smr_sleep_nanoseconds(uint64_t nanoseconds);
int smr_pid_is_alive(int32_t pid);
int32_t smr_current_pid(void);
int smr_detach_session(void);

void smr_install_termination_handlers(void);
int smr_should_terminate(void);

uint32_t smr_crc32(const void *bytes, uint64_t count);
int32_t smr_error_code(void);
int smr_bytes_equal(const void *left, const void *right, uint64_t count);
uint64_t smr_peak_resident_bytes(void);

#ifdef __cplusplus
}
#endif

#endif
