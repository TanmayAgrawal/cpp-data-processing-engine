#pragma once

#include <condition_variable>
#include <cstddef>
#include <future>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace dpe::detail {

class thread_pool {
 public:
  explicit thread_pool(std::size_t thread_count);
  thread_pool(const thread_pool&) = delete;
  auto operator=(const thread_pool&) -> thread_pool& = delete;
  ~thread_pool();

  [[nodiscard]] auto size() const noexcept -> std::size_t;
  auto enqueue(std::function<void()> task) -> std::future<void>;

 private:
  void worker_loop();

  std::vector<std::thread> workers_;
  std::queue<std::packaged_task<void()>> tasks_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_{false};
};

}  // namespace dpe::detail
