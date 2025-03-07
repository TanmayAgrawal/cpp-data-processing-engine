#include "dpe/detail/thread_pool.hpp"

#include <stdexcept>

namespace dpe::detail {

thread_pool::thread_pool(std::size_t thread_count) {
  if (thread_count == 0U) {
    throw std::invalid_argument("thread_pool requires at least one worker thread.");
  }

  workers_.reserve(thread_count);
  for (std::size_t index = 0U; index < thread_count; ++index) {
    workers_.emplace_back([this] { worker_loop(); });
  }
}

thread_pool::~thread_pool() {
  {
    std::scoped_lock lock{mutex_};
    stopping_ = true;
  }

  cv_.notify_all();

  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

auto thread_pool::size() const noexcept -> std::size_t {
  return workers_.size();
}

auto thread_pool::enqueue(std::function<void()> task) -> std::future<void> {
  std::packaged_task<void()> packaged{std::move(task)};
  auto future = packaged.get_future();

  {
    std::scoped_lock lock{mutex_};
    if (stopping_) {
      throw std::runtime_error("enqueue on stopped thread_pool");
    }

    tasks_.push(std::move(packaged));
  }

  cv_.notify_one();
  return future;
}

void thread_pool::worker_loop() {
  while (true) {
    std::packaged_task<void()> task;

    {
      std::unique_lock lock{mutex_};
      cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });

      if (stopping_ && tasks_.empty()) {
        return;
      }

      task = std::move(tasks_.front());
      tasks_.pop();
    }

    task();
  }
}

}  // namespace dpe::detail
