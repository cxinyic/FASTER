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
    core::IAsyncContext* context;  
};
template <uint64_t S, uint64_t C, uint64_t P>
class LocalMemoryFile {
  private:
    int num_segments;
    concurrent_queue<IORequestLocalMemory> * ioQueue;
   
  public:
    typedef LocalMemoryHandler handler_t;
    uint8_t ** ram_segments;
    bool terminated = false;
    std::thread * ioProcessors;

    static constexpr uint64_t kSegmentSize = S;
    static_assert(core::Utility::IsPowerOfTwo(S), "template parameter S is not a power of two!");

    static constexpr uint64_t kCapacity = C;
    static_assert(core::Utility::IsPowerOfTwo(C), "template parameter C is not a power of two!");

    static constexpr uint64_t kParallelism = P;
    static void ProcessIOQueue(concurrent_queue<IORequestLocalMemory> q)
    {
        printf("ProcessIOqueue\n");
        struct IORequestLocalMemory req; 
        while (1)
        {
            while (q.try_pop(req))
            {   
                printf("ProcessIOQueue poping\n");
                std::memcpy(req.srcAddress, req.dstAddress, req.length);
                req.callback(req.context, core::Status::Ok, req.length);
            }
            std::this_thread::yield();
        }
    }


    LocalMemoryFile(){
        
        num_segments = (int)(kCapacity / kSegmentSize);
        ram_segments = (uint8_t **) std::malloc(sizeof(uint8_t*) * num_segments);
        for (int i = 0; i < num_segments; i++){
            ram_segments[i] = (uint8_t *) std::malloc(sizeof(uint8_t) * kSegmentSize);
        } 

        terminated = false;
        ioQueue = new concurrent_queue<IORequestLocalMemory>[kParallelism];
        ioProcessors = (std::thread*) std::malloc(sizeof(std::thread) * kParallelism);
        
        /*for (int i = 0; i < kParallelism; i++)
        {
            auto x = i;
            ioProcessors[i] = std::thread(ProcessIOQueue, ioQueue[x]);
        }*/
    } 

    ~LocalMemoryFile(){
        /*for(int i = 0; i < kParallelism; i++) {
          ioProcessors[i].join();
        }*/
        for (int i = 0; i < num_segments; i++){
            free(ram_segments[i]);
        }
        free(ram_segments);
    }
    
    core::Status Open(LocalMemoryHandler* handler) {
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
        IORequestLocalMemory req {
          .srcAddress = ram_segments[segment] + source % kSegmentSize,
          .dstAddress = (void*)dest,
          .length = length,
          .callback = callback,
          .context = &context
        };
        /*
        req.srcAddress = ram_segments[segment] + source % kSegmentSize;
        req.dstAddress = (void*)dest;
        req.length = length;
        req.callback = callback;
        req.context = context;*/
        //q.push(req);
        core::IAsyncContext* caller_context_copy;
        context.DeepCopy(caller_context_copy);
        std::memcpy(req.dstAddress, req.srcAddress, req.length);
        callback(caller_context_copy, core::Status::Ok, req.length);
    }
    core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                    core::AsyncIOCallback callback, core::IAsyncContext& context) {
        uint64_t segment = dest / kSegmentSize;
        auto q = ioQueue[segment % kParallelism];
        IORequestLocalMemory req {
          .srcAddress = (void*)source,
          .dstAddress = ram_segments[segment] + dest % kSegmentSize,
          .length = length,
          .callback = callback,
          .context = &context
        };
       /* req.srcAddress = (void*)source;
        req.dstAddress = ram_segments[segment] + dest % kSegmentSize;
        req.length = length;
        req.callback = callback;
        req.context = context;*/
        //q.push(req);
        core::IAsyncContext* caller_context_copy;
        context.DeepCopy(caller_context_copy);
        std::memcpy( req.dstAddress, req.srcAddress, req.length);
        callback(caller_context_copy, core::Status::Ok, req.length);   
    }

    static size_t alignment() {
        // Align null device to cache line.
        return 32;
    }

    void set_handler(LocalMemoryHandler* handler) {
    }

};
template <uint64_t S, uint64_t C, uint64_t P>
class LocalMemoryDisk {
 public:
  typedef LocalMemoryHandler handler_t;
  typedef LocalMemoryFile<S, C, P> file_t;
  typedef LocalMemoryFile<S, C, P> log_file_t;

  LocalMemoryDisk(const std::string& filename, core::LightEpoch& epoch,
           const std::string& config){
  }

  static uint32_t sector_size() {
    return 32;
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

  log_file_t NewFile(const std::string& relative_path) {
    assert(false);
    return log_file_t{};
  }

  handler_t& handler() {
    return handler_;
  }

  inline static constexpr bool TryComplete() {
    return true;
  }

 private:
  handler_t handler_;
  log_file_t log_;
};


}
} // namespace FASTER::device