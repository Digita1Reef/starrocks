// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/binary_column.h"
#include "common/config.h"
#include "common/status.h"
#include "fmt/format.h"

namespace starrocks::serde::detail {

// A binary column is only well-formed when its offsets describe its byte payload: there is at least the
// leading offset, the first one is zero, they never decrease, and the last one equals the payload size.
// Consumers such as `append_selective` and `get_slice` index the byte buffer straight from the offsets,
// so a buffer that violates any of this must be rejected here, at the boundary where it enters the process.
inline Status binary_column_corruption(const std::string& msg) {
    if (config::enable_dcheck_on_serde_failure) {
        DCHECK(false) << msg;
    }
    return Status::Corruption(msg);
}

template <typename T>
inline Status check_binary_offsets(const Buffer<T>& offsets, size_t bytes_size) {
    if (UNLIKELY(offsets.empty())) {
        return binary_column_corruption("binary column has no offsets");
    }
    if (UNLIKELY(offsets.front() != 0)) {
        return binary_column_corruption(fmt::format("binary column first offset is {}, expected 0", offsets.front()));
    }
    if (UNLIKELY(offsets.back() != bytes_size)) {
        return binary_column_corruption(fmt::format("binary column last offset {} does not match byte payload size {}",
                                                    offsets.back(), bytes_size));
    }
    const T* data = offsets.data();
    const size_t num_offsets = offsets.size();
    bool non_decreasing = true;
    for (size_t i = 1; i < num_offsets; ++i) {
        non_decreasing &= (data[i - 1] <= data[i]);
    }
    if (UNLIKELY(!non_decreasing)) {
        return binary_column_corruption(
                fmt::format("binary column offsets are not non-decreasing, num_offsets: {}, byte payload size: {}",
                            num_offsets, bytes_size));
    }
    return Status::OK();
}

template <typename T>
inline Status check_offset_byte_count(size_t byte_count) {
    if (UNLIKELY(byte_count % sizeof(T) != 0)) {
        return binary_column_corruption(fmt::format("binary column offset payload size {} is not a multiple of {}",
                                                    byte_count, sizeof(T)));
    }
    return Status::OK();
}

} // namespace starrocks::serde::detail
