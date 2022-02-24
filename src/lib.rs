use bollard::Docker;
use bollard::container::{
    Config,
    CreateContainerOptions,
    StartContainerOptions,
    WaitContainerOptions,
};
use bollard::models::HostConfig;
use tokio::fs;

use std::default::Default;
use futures_util::stream::TryStreamExt;
use std::fmt;
use std::io;
use std::path::Path;
// use futures_util::future::FutureExt;
use daggy::Dag;


// type DagrError = Box<dyn Error + 'static>;
const DATADIR: &str = "/tmp/dagr/data";
type DagrResult<'a> = Result<DagrResultData, DagrError>;
type DagrGraph<'a> = Dag::<DagrNode<'a>, u32, u32>;


fn graph<'a>() -> Result<DagrGraph<'a>, Box<dyn std::error::Error>> {
    let mut dag = DagrGraph::new();
    let name = "foo";
    let sleep = 10;
    let cli_cmd = format!("sleep {} && echo 'hi {}' > {}.txt", sleep, name, name).as_str();
    let root_input = Execution::new(name, cli_cmd);
    let root_idx = dag.add_node(DagrNode::List);
    dag.add_child(root_idx, 0, DagrNode::Processor(root_input));
    Ok(dag)
    
}

enum DagrNode<'a> {
    List,
    Processor(Execution<'a>),
}

struct Execution<'a> {
    name: &'a str,
    config: Config<&'a str>,
}

impl <'a> Execution<'a> {
    pub fn new(name: &'a str, cli: &'a str) -> Execution<'a> {

        let container_workdir = Path::new(DATADIR).join(name);
        let dir_string = container_workdir.to_string_lossy();

        return Self {
            name,
            config: Config {
                cmd: Some(vec!["-c", cli]),
                entrypoint: Some(vec!["sh"]),
                host_config: Some(HostConfig{
                    memory: Some(128 * 1024 * 1024),
                    cpu_quota: Some(100_000),
                    cpu_period: Some(100_000),
                    binds: Some(vec![format!("{}:/workdir", dir_string)]),
                    ..Default::default()
                }),
                image: Some("alpine:latest"),
                working_dir: Some("/workdir"),
                ..Default::default()
            }
        }
    }
}

#[derive(Debug)]
enum DagrError {
    Io(io::Error),
    Container(bollard::errors::Error),
}

impl fmt::Display for DagrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DagrError::Io(ref err) => write!(f, "dagr io error: {}", err),
            DagrError::Container(ref err) => write!(f, "dagr container error: {}", err),
        }
    }
}

impl From<io::Error> for DagrError {
    fn from(err: io::Error) -> DagrError {
        DagrError::Io(err)
    }
}

impl From<bollard::errors::Error> for DagrError {
    fn from(err: bollard::errors::Error) -> DagrError {
        DagrError::Container(err)
    }
}

#[derive(Debug)]
struct DagrFile {
    path: String,
    size: usize,
    checksum: u64,
}

#[derive(Debug)]
struct DagrResultData {
    name: String,
    exit_code: i64,
    files: Vec<DagrFile>,
}

async fn execute_container<'a>(docker: &Docker, name: String, sleep: u8) -> DagrResult<'a> {
    let container_workdir = Path::new(DATADIR).join(name.as_str());
    let dir_string = container_workdir.to_string_lossy();
    fs::create_dir_all(&container_workdir).await?;

    let opts = CreateContainerOptions {
        name: name.as_str(),
    };
    let cli_cmd = format!("sleep {} && echo 'hi {}' > {}.txt", sleep, name, name);
    let config = Config {
        cmd: Some(vec!["-c", cli_cmd.as_str()]),
        entrypoint: Some(vec!["sh"]),
        host_config: Some(HostConfig{
            memory: Some(128 * 1024 * 1024),
            cpu_quota: Some(100_000),
            cpu_period: Some(100_000),
            binds: Some(vec![format!("{}:/workdir", dir_string)]),
            ..Default::default()
        }),
        image: Some("alpine:latest"),
        working_dir: Some("/workdir"),
        ..Default::default()
    };

    docker.create_container(Some(opts), config).await?;
    println!("Before start ({})", name);
    docker.start_container(&name, None::<StartContainerOptions<String>>).await?;
    println!("After start ({})", name);
    let wait_opts = Some(WaitContainerOptions { condition: "not-running" });
    let responses = docker.wait_container(&name, wait_opts).try_collect::<Vec<_>>().await?;

    let response = &responses[0];

    let result = DagrResultData {
        name,
        exit_code: response.status_code,
        files: vec![DagrFile { path: dir_string.to_string(), size: 0, checksum: 0 }],
    };

    Ok(result)
}

async fn run_all() -> Result<(), DagrError> {
    // let tasks = (0..10).map(|i| execute_container(&docker, i.to_string().as_str())).collect::<Vec<Future<Output=DagrResult>>>>();
    let mut tasks = vec![];
    for i in 0..10 {
        // let name = i.to_string();
        let task = tokio::spawn(async move {
            let name = format!("container-{}", i);
            let docker = Docker::connect_with_local_defaults().unwrap();
            execute_container(&docker, name.to_string(), 5).await.unwrap()
        });
        tasks.push(task);
    }

    let task = tokio::spawn(async move {
        let docker = Docker::connect_with_local_defaults().unwrap();
        execute_container(&docker, "foo".to_string(), 1).await.unwrap()
    });
    tasks.push(task);

    for task in tasks.drain(..) {
        let result = task.await.unwrap();
        println!("Container {} exit code: {} ({:?})", result.name, result.exit_code, result.files);
    }  

    println!("Done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // #[tokio::test]
    // async fn it_works() {
    //     run_all().await.unwrap();
    // }

    #[test]
    fn test_dag() {
        graph().unwrap();
    }
}
