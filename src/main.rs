use actix::{Message, Actor, Handler, System, Addr, SyncArbiter, SyncContext};
use actix::dev::{MessageResponse, OneshotSender};
use std::collections::VecDeque;
use std::time;
use std::{path::Path, fs};


// count files using bfs
fn count_files(dir: &str) -> i32 {
    if Path::new(dir).is_file() {
        return 1;
    }
    let mut dq = VecDeque::new();
    let mut count = 0;
    dq.push_back(dir.to_string());

    while !dq.is_empty() {
        let size = dq.len();
        for _ in 0..size {
            let re = dq.pop_front().unwrap();
            let _path = Path::new(&re);
            if _path.is_file() {
                count += 1;
            }
            else{
                for file in fs::read_dir(&_path).unwrap(){
                    match file {
                        Ok(file_path) => {
                            if file_path.path().is_file(){
                                count += 1;
                            }
                            else {
                                dq.push_back(file_path.path().to_str().unwrap().to_string());
                            }
                        },
                        Err(_) => {}
                    }
                }
            }
        }
    }
    count
}


// Message to the actor will contain the path as string
#[derive(Message)]
#[rtype(result="Response")]
struct FilePath(String);


// Response of the actor will contain the count of files and directories in a given directory
#[derive(Clone)]
struct Response{
    count: i32,
    directories: Vec<String>
}

impl <A, M> MessageResponse<A, M> for Response 
where 
    A: Actor,
    M: Message<Result = Response>
{
    fn handle(self, _ctx: &mut A::Context, tx: Option<OneshotSender<M::Result>>) {
        if let Some(tx) = tx {
            match tx.send(self){
                Ok(_) => {},
                Err(_) => {
                    println!("Sending response failed");
                }
            };
        }
    }
}

// Actor
struct FileCounter;

// Using sync context as work is I/O bound
impl Actor for FileCounter {
    type Context = SyncContext<Self>;
}

impl Handler<FilePath> for FileCounter{
    type Result = Response;

    fn handle(&mut self, msg: FilePath, _ctx: &mut Self::Context) -> Self::Result {
        let mut count = 0;
        let mut directories: Vec<String> = vec![];
        
        for file in fs::read_dir(msg.0).unwrap(){
            match file {
                Ok(file_path) => {
                    if file_path.path().is_file(){
                        count += 1;
                    }
                    else {
                        directories.push(file_path.path().to_str().unwrap().to_string());
                    }
                },
                Err(_) => {}
            }
        }
        
        return Response{count, directories};
    }
}


async fn get_duration_actix(dir: &str, num_threads: usize) -> time::Duration{
    let addr: Addr<FileCounter> = SyncArbiter::start(num_threads, || FileCounter);
    let st = time::Instant::now();
    let mut count = 0;
    let mut dq = VecDeque::new();
    dq.push_back(addr.send(FilePath(dir.to_string())));

    while !dq.is_empty() {
        let size = dq.len();
        for _ in 0..size {
            let re = dq.pop_front().unwrap().await.unwrap();
            count += re.count;
            for c in re.directories {
                dq.push_back(addr.send(FilePath(c)));
            }
        }
    }
    println!("{}", count);
    time::Instant::now().duration_since(st)
}


fn get_duration_sequential(dir: &str) -> time::Duration {
    let st = time::Instant::now();
    println!("{}", count_files(dir));
    time::Instant::now().duration_since(st)
}


#[actix_rt::main] 
async fn main() {
    let dir = "./target";    
    let num_threads = 4;
    let time_seq = get_duration_sequential(dir);
    let time_actix = get_duration_actix(dir, num_threads).await;
    println!("Sequential execution took {:?}", time_seq);
    println!("Execution using actix took {:?}", time_actix);
    println!("Execution improved by a factor of {:.4} after using 4 threads", time_seq.as_secs_f64()/time_actix.as_secs_f64());
    System::current().stop();
}
