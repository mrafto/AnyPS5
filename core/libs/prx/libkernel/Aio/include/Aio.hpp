#ifndef CORE_LIBS_PRX_LIBKERNEL_AIO_INCLUDE_AIO_HPP
#define CORE_LIBS_PRX_LIBKERNEL_AIO_INCLUDE_AIO_HPP

#include <cstdint>

namespace Aio {

using TransferGate = void (*)(std::int32_t fd, bool write, std::int64_t offset);

void DrainDescriptor(std::int32_t fd);
void SetTransferGate_nid_no_patch(TransferGate gate);

}

#endif
