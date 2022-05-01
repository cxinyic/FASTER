// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "../core/gc_state.h"
#include "../core/light_epoch.h"
#include "../core/guid.h"
#include "../environment/file.h"

#ifdef _WIN32

#include <concurrent_unordered_map.h>
template <typename K, typename V>
using concurrent_unordered_map = concurrency::concurrent_unordered_map<K, V>;

#include <concurrent_queue.h>
template <typename T>
using concurrent_queue = concurrency::concurrent_queue<T>;

#else

#include "tbb/concurrent_unordered_map.h"
template <typename K, typename V>
using concurrent_unordered_map = tbb::concurrent_unordered_map<K, V>;

#include "tbb/concurrent_queue.h"
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;

#endif

namespace FASTER {
namespace device {

struct LocalMemoryHandler {
};

struct IORequestLocalMemory 
{
    void* srcAddress;
    void* dstAddress;
    uint32_t length;
    core::AsyncIOCallback callback;
    core::IAsyncContext& context;
    
}
template <uint64_t S, uint64_t C, uint64_t P>
class LocalMemoryFile {
  private:
    int num_segments;
    concurrent_queue<IORequestLocalMemory> * ioQueue;
    std::deque<std::thread> ioProcessors;
    bool terminated;

    void ProcessIOQueue(concurrent_queue<IORequestLocalMemory> q)
    {
        struct IORequestLocalMemory req; 
        while (terminated == false)
        {
            while (q.try_pop(req))
            {
                std::memcpy(req.srcAddress, req.dstAddress, req.length);
                callback(req.context, core::Status::Ok, req.length);
            }
            std::this_thread::yield();
        }
    }

  public:
    typedef LocalMemoryHandler handler_t;
    uint8_t ** ram_segments;

    static constexpr uint64_t kSegmentSize = S;
    static_assert(core::Utility::IsPowerOfTwo(S), "template parameter S is not a power of two!");

    static constexpr uint64_t kCapacity = C;
    static_assert(core::Utility::IsPowerOfTwo(C), "template parameter C is not a power of two!");

    static constexpr uint64_t kParallelism = P;

    LocalMemoryFile(){
        num_segments = (int)(kParallelism / KSegmentSize);
        ram_segments = (uint8_t **) std::malloc(sizeof(uint8_t*) * num_segments);
        for (int i = 0; i < num_segments; i++){
            ram_segments[i] = (uint8_t *) std::malloc(sizeof(uint8_t) * kSegmentSize);
        } 

        terminated = False;
        ioQueue = new concurrent_queue<IORequestLocalMemory>[kParallelism];
        ioProcessors = new std::thread[kParallelism];
        for (int i = 0; i < kParallelism; i++)
        {
            auto x = i;
            ioQueue[x] = new concurrent_queue<IORequestLocalMemory>();
            ioProcessors.emplace_back(&ProcessIOQueue, ioQueue[x]);
        }

    } 

    ~LocalMemoryFile(){
        terminated = true;
        for(auto& thread : ioProcessors) {
            thread.join();
        }
        for (int i = 0; i < num_segments; i++){
            free(ram_segments[i]);
        }
        free(ram_segments);
    }
    
    core::Status Open(NullHandler* handler) {
        return core::Status::Ok;
    }
    core::Status Close() {
        return core::Status::Ok;
    }
    core::Status Delete() {
        return core::Status::Ok;
    }
    void Truncate(uint64_t new_begin_offset, core::GcState::truncate_callback_t callback) {
        if(callback) {
            callback(new_begin_offset);
        }
    }

    core::Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                   core::AsyncIOCallback callback, core::IAsyncContext& context) const {
        
        uint64_t segment = source / kSegmentSize;
        auto q = ioQueue[segment % kParallelism];
        auto req = new IORequestLocalMemory 
        {
            srcAddress = ram_segments[segment] + source % kSegmentSize,
            dstAddress = (void*)dest,
            length = length,
            callback = callback,
            context = context
        };
        q.push(req);
    }
    core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                    core::AsyncIOCallback callback, core::IAsyncContext& context) {
        uint64_t segment = dst / kSegmentSize;
        auto q = ioQueue[segment % kParallelism];
        auto req = new IORequestLocalMemory 
        {
            srcAddress = (void*)source,
            dstAddress = ram_segments[segment] + dst % kSegmentSize,
            length = length,
            callback = callback,
            context = context
        };
        q.push(req);
    }

    static size_t alignment() {
        // Align null device to cache line.
        return 64;
    }

    void set_handler(LocalMemoryHandler* handler) {
    }
  
  


};
template <uint64_t S, uint64_t C, uint64_t P>
class LocalMemoryDisk {
 public:
  typedef LocalMemoryHandler handler_t;
  typedef LocalMemoryFile<S, C, P> log_file_t;

  LocalMemoryDisk(const std::string& filename, core::LightEpoch& epoch,
           const std::string& config){
  }

  static uint32_t sector_size() {
    return 64;
  }

  /// Methods required by the (implicit) disk interface.
  const log_file_t& log() const {
    return log_;
  }
  log_file_t& log() {
    return log_;
  }

  std::string relative_index_checkpoint_path(const core::Guid& token) const {
    assert(false);
    return "";
  }
  std::string index_checkpoint_path(const core::Guid& token) const {
    assert(false);
    return "";
  }

  std::string relative_cpr_checkpoint_path(const core::Guid& token) const {
    assert(false);
    return "";
  }
  std::string cpr_checkpoint_path(const core::Guid& token) const {
    assert(false);
    return "";
  }

  void CreateIndexCheckpointDirectory(const core::Guid& token) {
    assert(false);
  }
  void CreateCprCheckpointDirectory(const core::Guid& token) {
    assert(false);
  }

  file_t NewFile(const std::string& relative_path) {
    assert(false);
    return file_t{};
  }

  handler_t& handler() {
    return handler_;
  }

  inline static constexpr bool TryComplete() {
    return false;
  }

 private:
  handler_t handler_;
  log_file_t log_;
};


}
} // namespace FASTER::device