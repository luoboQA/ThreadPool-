#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

class ThreadPool {
public:
    // 构造函数：创建指定数量的工作线程
    ThreadPool(size_t);
    // 将任务（可调用对象及其参数）提交到队列中异步执行，\
    返回一个 future对象,用于等待任务完成并获取返回值
    /*
    模板参数：
    F：可调用对象的类型（函数、函数对象、lambda 表达式等）
    Args：可调用对象的参数类型列表
    参数：
    f：要执行的可调用对象
    args：传递给可调用对象的参数
    返回值：
    std::future<...> - 异步结果包装器，用于获取任务执行结果
    < > - 模板参数列表，指定 F 和 Args 的类型
    () - 调用运算符，表示调用可调用对象
    typename 告诉编译器某个依赖名称是类型
    :: - 作用域解析运算符，用于访问某个作用域中的名称
    <F(Args...)> - 给定一个F类型的可调用对象，用Args类型的参数调用它
    typename std::result_of<F(Args...)>::type - 推导 f(args...) 的返回类型
    */
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) 
        -> std::future<typename std::result_of<F(Args...)>::type>;
    // 析构函数：停止所有线程并等待它们结束
    ~ThreadPool();
private:
    // need to keep track of threads so we can join them
    std::vector< std::thread > workers;
    // the task queue,存储待执行的函数对象
    std::queue< std::function<void()> > tasks;
    
    // synchronization
    std::mutex queue_mutex; // 保护任务队列的互斥锁
    std::condition_variable condition; // 用于线程等待/唤醒的条件变量
    bool stop; // 是否停止线程池的标志
};
 
// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
    :   stop(false)
{
    // 创建 threads 个工作线程，每个线程不断从任务队列中取出任务并执行
    for(size_t i = 0;i<threads;++i){
        // 每次循环都创建一个新的 std::thread 对象，并将其添加到 workers 中
        workers.emplace_back([this]{ // 创建线程，传入lambda函数,定义每个工作线程要执行的代码逻辑 \
        // 每个线程都捕获同一个 this,即当前的 ThreadPool 对象，以便在线程中访问成员变量和成员函数
        // emplace_back直接构造一个 std::thread 对象并放入容器
                for(;;)
                {
                    std::function<void()> task; //定义一个函数对象，用于存储从任务队列中取出的任务

                    {
                        // 获取锁，保护"取任务"这个过程
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        
                        // 等待条件变量 让线程休眠直到被唤醒，避免空转浪费CPU：直到线程池停止 或 任务队列非空
                        // wait函数会自动释放锁，并在被唤醒后重新获取锁，继续执行后续代码
                        this->condition.wait(lock, [this]{ 
                            return this->stop || !this->tasks.empty();
                         }); 
                        
                        // 如果线程池已停止且任务队列为空，则退出线程
                        if(this->stop && this->tasks.empty())
                            return;
                        // 取出任务队列中的第一个任务    
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    // 执行任务（不持有锁）
                    task();
                }
        });
    }     
}

// add new work item to the pool，返回一个 future 让用户获取结果，任务提交后不会立即返回结果（因为任务可能还没执行），但立即返回一个"凭证"（future），将来你可以用这个凭证去领取结果
template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args) 
    -> std::future<typename std::result_of<F(Args...)>::type>
{   
    // 定义返回值类型
    using return_type = typename std::result_of<F(Args...)>::type;

    // 将任务封装为 packaged_task，便于获取 future
    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
    
    // 获取 future 对象
    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // 不允许在停止后继续提交任务
        if(stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");

        // 将任务包装为 void() 函数并放入队列
        tasks.emplace([task](){ (*task)(); });
    }
    // 唤醒一个工作线程来执行新提交的任务
    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool()
{
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true; // 设置停止标志
    }
    condition.notify_all(); // 唤醒所有同一个条件变量的线程，让它们检查停止标志并退出
    for(std::thread &worker: workers)
        worker.join(); // 主线程在这里等待，等待每个线程退出
}

#endif
