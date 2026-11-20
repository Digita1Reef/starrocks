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

#include <string>
#include <string_view>

#include "common/status.h"

namespace starrocks {

// Routine load renews aborted tasks. The PAUSE marker is reserved for errors that need operator intervention.
inline Status pulsar_assignment_error(std::string_view operation, pulsar::Result result) {
    std::string message(operation);
    message.append(": ").append(pulsar::strResult(result));
    switch (result) {
    case pulsar::ResultConnectError:
    case pulsar::ResultReadError:
    case pulsar::ResultNotConnected:
    case pulsar::ResultDisconnected:
    case pulsar::ResultTimeout:
    case pulsar::ResultServiceUnitNotReady:
        return Status::ServiceUnavailable(message);
    default:
        return Status::InternalError("PAUSE: " + message);
    }
}

} // namespace starrocks
