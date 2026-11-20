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

#include <pulsar/Result.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include "common/status.h"

namespace starrocks {

// Routine load renews aborted tasks. The PAUSE marker is reserved for errors that need operator intervention.
inline Status pulsar_assignment_error(std::string_view operation, pulsar::Result result,
                                      const std::map<std::string, int64_t>& initial_positions) {
    std::string message(operation);
    message.append(": ").append(pulsar::strResult(result));
    switch (result) {
    case pulsar::ResultConnectError:
    case pulsar::ResultReadError:
    case pulsar::ResultNotConnected:
    case pulsar::ResultDisconnected:
    case pulsar::ResultTimeout:
    case pulsar::ResultServiceUnitNotReady:
        // A replacement task must not repeat an explicit seek on another partition it already assigned.
        if (std::none_of(initial_positions.begin(), initial_positions.end(),
                         [](const auto& entry) { return entry.second >= 0; })) {
            return Status::ServiceUnavailable(message);
        }
        break;
    default:
        break;
    }
    return Status::InternalError("PAUSE: " + message);
}

} // namespace starrocks
