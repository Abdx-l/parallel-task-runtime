#include "runtime.hpp"
#include <cassert>
#include <random>
int main(){Runtime r(4);std::vector<int>a(1000000);std::mt19937 rng(42);for(auto&x:a)x=int(rng());auto b=a;std::sort(b.begin(),b.end());r.merge_sort(a);assert(a==b);std::atomic<int>count{0};r.parallel_for(100000,[&](size_t){count++;});assert(count==100000);std::vector<std::future<void>>parents;for(int i=0;i<100;i++)parents.push_back(r.submit([&]{for(int j=0;j<50;j++)r.submit([&]{count++;});}));for(auto&f:parents)f.get();}
