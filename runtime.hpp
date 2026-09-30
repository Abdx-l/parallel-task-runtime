#pragma once
#include <atomic>
#include <array>
#include <algorithm>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <stdexcept>
struct Task{std::function<void()> run;};
class Deque{
 static constexpr long capacity=65536;
 std::array<std::atomic<Task*>,capacity> buffer{};
 std::atomic<long> top{0},bottom{0};
public:
 bool push(Task*task){long b=bottom.load(std::memory_order_relaxed),t=top.load(std::memory_order_acquire);if(b-t>=capacity-1)return false;buffer[b%capacity].store(task,std::memory_order_relaxed);std::atomic_thread_fence(std::memory_order_release);bottom.store(b+1,std::memory_order_relaxed);return true;}
 Task*pop(){long b=bottom.load(std::memory_order_relaxed)-1;bottom.store(b,std::memory_order_relaxed);std::atomic_thread_fence(std::memory_order_seq_cst);long t=top.load(std::memory_order_relaxed);Task*result=nullptr;
 if(t<=b){result=buffer[b%capacity].load(std::memory_order_relaxed);if(t==b){if(!top.compare_exchange_strong(t,t+1,std::memory_order_seq_cst))result=nullptr;bottom.store(b+1,std::memory_order_relaxed);}}
 else bottom.store(b+1,std::memory_order_relaxed);return result;}
 Task*steal(){long t=top.load(std::memory_order_acquire);std::atomic_thread_fence(std::memory_order_seq_cst);long b=bottom.load(std::memory_order_acquire);if(t>=b)return nullptr;Task*result=buffer[t%capacity].load(std::memory_order_relaxed);return top.compare_exchange_strong(t,t+1,std::memory_order_seq_cst)?result:nullptr;}
};
class Runtime{
 std::vector<std::unique_ptr<Deque>> queues;std::vector<std::thread> workers;
 std::mutex mutex;std::deque<Task*> injected;std::vector<std::unique_ptr<Task>> owned;
 std::atomic<size_t> pending{0};std::atomic<bool> stopping{false};
 inline static thread_local Runtime*current=nullptr;inline static thread_local size_t worker_id=0;
 Task*next(size_t id){
  if(auto*t=queues[id]->pop())return t;
  {std::lock_guard<std::mutex>lock(mutex);if(!injected.empty()){auto*t=injected.front();injected.pop_front();return t;}}
  for(size_t i=1;i<queues.size();i++)if(auto*t=queues[(id+i)%queues.size()]->steal())return t;
  return nullptr;
 }
public:
 explicit Runtime(size_t count=std::thread::hardware_concurrency()){
  count=std::max<size_t>(1,count);
  for(size_t i=0;i<count;i++)queues.emplace_back(std::make_unique<Deque>());
  for(size_t i=0;i<count;i++)workers.emplace_back([this,i]{current=this;worker_id=i;while(!stopping||pending){if(auto*t=next(i)){t->run();pending.fetch_sub(1);}else std::this_thread::yield();}current=nullptr;});
 }
 template<class F> auto submit(F f)->std::future<decltype(f())>{
  auto packaged=std::make_shared<std::packaged_task<decltype(f())()>>(std::move(f));auto future=packaged->get_future();
  auto task=std::make_unique<Task>();task->run=[packaged]{(*packaged)();};auto*raw=task.get();
  {std::lock_guard<std::mutex>lock(mutex);if(stopping)throw std::runtime_error("runtime stopped");owned.push_back(std::move(task));pending++;}
  if(current==this&&queues[worker_id]->push(raw))return future;
  {std::lock_guard<std::mutex>lock(mutex);injected.push_back(raw);}return future;
 }
 template<class F>void parallel_for(size_t n,F fn,size_t grain=1024){
  if(!grain)throw std::invalid_argument("grain must be positive");std::vector<std::future<void>>done;
  for(size_t b=0;b<n;b+=grain)done.push_back(submit([=]{for(size_t i=b;i<std::min(n,b+grain);i++)fn(i);}));
  for(auto&f:done)f.get();
 }
 template<class T>void merge_sort(std::vector<T>&values,size_t grain=4096){
  if(!grain)throw std::invalid_argument("grain must be positive");
  parallel_for((values.size()+grain-1)/grain,[&](size_t block){size_t b=block*grain;std::sort(values.begin()+b,values.begin()+std::min(values.size(),b+grain));},1);
  for(size_t width=grain;width<values.size();width*=2)parallel_for((values.size()+2*width-1)/(2*width),[&](size_t block){size_t b=block*2*width,m=std::min(values.size(),b+width),e=std::min(values.size(),b+2*width);std::inplace_merge(values.begin()+b,values.begin()+m,values.begin()+e);},1);
 }
 ~Runtime(){while(pending)std::this_thread::yield();stopping=true;for(auto&t:workers)t.join();}
};
