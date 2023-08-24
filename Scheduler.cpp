#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>
#include <semaphore>
#include <set>
#include <map>
#include <vector>
#include <atomic>

using namespace std;

chrono::time_point<chrono::steady_clock> start;

class Task{
    public:
    int task_id;
    int service_type;
    int resources_required;
    chrono::time_point<chrono::steady_clock> start_time;
    bool blocked_no_workers;
    bool blocked_no_resources;
    bool rejected;

    Task(int id, int type, int res){
        task_id = id;
        service_type = type;
        resources_required = res;
        blocked_no_resources = false;
        blocked_no_workers = false;
        rejected = false;
    }
};

class Service;
class Worker{
    int worker_id;
    Service* service;
    int priority;
    int resources;
    thread worker_thread;
    condition_variable start_working;
    condition_variable done_working;
    mutex mtx;
    Task* task;
    bool is_working;
    bool alive;

    public:
    //struct to order the workers based on priority.
    struct OrderByPriority{
        bool operator()(const Worker* w1, const Worker* w2) const{
            return (w1->get_priority() > w2->get_priority());
        }
    };

    Worker(int worker_id, int priority, int resources);
    ~Worker();
    void work();
    void allot_work(Task* t, Service* s);
    int get_resources() const;
    int get_id() const;
    int get_priority() const;
};

class Service{
    int service_type;
    int m;
    int max_resource;
    atomic<int> available_workers;
    mutex mtx;
    map<int, set<Worker*, Worker::OrderByPriority>> workers;
    condition_variable worker_added;

    public:
    Service(int type, int m, vector<Worker*>& threads);
    void insert_worker(Worker* worker);
    Worker* request_service(Task* t);
};

/**
 * Creates a worker instance.
 * 
 * @param worker_id integer id of the worker
 * @param priority priority of the thread
 * @param resources resources available
 */
Worker::Worker(int worker_id, int priority, int resources) : worker_id(worker_id), 
                                                         priority(priority), 
                                                         resources(resources),
                                                         alive(true),
                                                         is_working(false){

    worker_thread = thread(&Worker::work, this);

}

Worker:: ~Worker(){
    is_working = true;
    start_working.notify_all();
    alive = false;
    worker_thread.join();
}

/**
 * performs the task once signalled. Once the task has been performed, inserts the worker into the service's worker set.
 */
void Worker::work(){
    while(alive){
        unique_lock<mutex> lock(mtx);
        while(!is_working)
            start_working.wait(lock);
    
        if(!alive)
            break;
        task->start_time = chrono::steady_clock::now();
        this_thread::sleep_for(chrono::milliseconds(100));
        //cout << "Worker " << worker_id << " working on " << task->task_id << endl;
        is_working = false;
        service->insert_worker(this);
        done_working.notify_all();
    }
}

/**
 * TSignals the thread to start working on the given task
 * 
 * @param t pointer to the task to perform.
 * @param s pointer to the service which requested work.
 */
void Worker::allot_work(Task* t, Service* s){
    task = t;
    service = s;
    start_working.notify_all();
    unique_lock<mutex> lock(mtx);
    is_working = true;
    done_working.wait(lock);
}

int Worker::get_id() const{
    return worker_id;
}

int Worker::get_resources() const{
    return resources;
}

int Worker::get_priority() const{
    return priority;
}

/**
 * Creates an instance of service class.
 * 
 * @param type Type id of the service.
 * @param m Number of workers.
 * @param threads A vector of size m that holds the pointer to workers.
 */
Service::Service(int type, int m, vector<Worker*>& threads) : service_type(type), m(m), 
                                                               available_workers(m){
    for(Worker* worker : threads){
        int resource = worker->get_resources();
        max_resource = max(max_resource, resource);
        workers[resource].insert(worker);
    }
}

/**
 * Inserts worker into the pool of workers. Wakes up all the process waiting for some worker to be added to the pool
 * 
 * @param worker Pointer to the worker instance to be inserted.
 */
void Service::insert_worker(Worker* worker){
    unique_lock<mutex> lock(mtx);
    //insert the worker into the set corresponding to the resource type.
    workers[worker->get_resources()].insert(worker);
    available_workers++;
    worker_added.notify_all();
}

/**
 * Request service to perform the task provided. Selects appropriate worker from the pool based on resource requirement and priority.
 * 
 * @param t Task to be performed.
 * @return pointer to a worker which can handle the task provided. NULL if the requirement can't be satisfied.
 */
Worker* Service::request_service(Task* t){
    //if resource requirement is greater than the maximum resource available in any worker, return NULL. 
    //mark the task as rejected.
    if(t->resources_required > max_resource){
        t->rejected = true;
        return NULL;
    }
    map<int, set<Worker*, Worker::OrderByPriority>>::iterator st;
    unique_lock<mutex> lock(mtx);

    //wait until some worker which can handle the task is added.
    while(available_workers == 0 || (st = workers.lower_bound(t->resources_required)) == workers.end()){
        if(available_workers == 0){
            cout << t->task_id << " : Worker not available\n";
            t->blocked_no_workers = true;
        }
        if(st == workers.end()){
            cout << t->task_id << " : Resource not available\n";
            t->blocked_no_resources = true;
        }
        worker_added.wait(lock);
    }

    //remove the worker from the pool and return.
    Worker* w = *(st->second.begin());
    st->second.erase(st->second.begin());
    if(st->second.empty())
        workers.erase(st);
    available_workers--;
    return w;
}

/**
 * Schedules the tasks using the workers available in the services.
 * 
 * @param services A vector of services available.
 * @param tasks A vector of tasks to be scheduled.
 */
void schedule(vector<Service*>& services, vector<Task*> tasks ){
    vector<thread*> threads;
    start = chrono::steady_clock::now();
    for(auto task : tasks){
        //get a worker that can handle the task.
        Worker* worker = services[task->service_type]->request_service(task);
        if(!worker){
            threads.push_back(NULL);
            continue;
        }
        //allot work to the thread.
        threads.push_back(new thread(&Worker::allot_work, worker, task, services[task->service_type]));
    }

    //wait until all the tasks have been completed.
    for(int i = 0; i < tasks.size(); i++){
        if(threads[i])
            threads[i]->join();
    }
}

int main(){
    int n;
    cin >> n;
    vector<Service*> services;
    for(int i = 0; i < n; i++){
        int m;
        cin >> m;
        vector<Worker*> w;
        for(int j = 0; j < m; j++){
            int p, r;
            cin >> p >> r;
            Worker* worker = new Worker(j+1, p, r);
            w.push_back(worker);
        }
        services.emplace_back(new Service(i, m, w));
    }

    int no_of_tasks;
    cin >> no_of_tasks;
    vector<Task*> tasks;

    for(int i = 0; i < no_of_tasks; i++){
        int t, r;
        cin >> t >> r;
        tasks.emplace_back(new Task(i+1, t, r));
    }

    schedule(services, tasks);

    int rejected = 0, no_workers = 0, no_resources = 0;
    chrono::duration<double> wait{0};
    cout << "\nstart times of each task\n";
    for(int i = 0; i < tasks.size(); i++){
        auto elapsed_time = std::chrono::duration_cast<std::chrono::nanoseconds>(tasks[i]->start_time - start);
        //cout << "Task " << tasks[i]->task_id << " - " << elapsed_time.count() << endl;
        
        if(tasks[i]->rejected){
            rejected++;
            cout << "Task " << tasks[i]->task_id << " - rejected" << endl;
        }
        else{
            cout << "Task " << tasks[i]->task_id << " - " << elapsed_time.count() << endl;
            wait += elapsed_time;
            if(tasks[i]->blocked_no_workers)
                no_workers++;
            if(tasks[i]->blocked_no_resources)
                no_resources++;
        }
    }

    auto avg = wait/tasks.size();

    cout << "\nAvg waiting time = " << avg.count() << " ns\n";
    cout << "Avg turnaround time = " << (avg+chrono::duration<double, milli>(100)).count() << "ns\n";
    cout << "The  number  of  requests  that  were  blocked  due  to  the  absence  of  any  available  worker threads = " << no_workers << endl;
    cout << "The number of requests that were forced to wait due to lack of available resources = " << no_resources << endl;
    cout << "Number of tasks rejected due to lack of resources = " << rejected << endl;
}


