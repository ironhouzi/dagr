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
use daggy::{Dag, Walker};


// type DagrError = Box<dyn Error + 'static>;
const DATADIR: &str = "/tmp/dagr/data";
type DagrResult<'a> = Result<DagrResultData, DagrError>;
type DagrGraph<'a> = Dag::<DagrNode, u32, u32>;


fn graph() -> Result<(DagrGraph<'static>, daggy::NodeIndex), Box<dyn std::error::Error>> {
    let mut dag = DagrGraph::new();
    let names = vec!["foo", "bar", "baz"];
    let sleep = 1;
    let root_idx = dag.add_node(DagrNode::List);

    for (i, name) in names.iter().enumerate() {
        let cli_cmd = format!("sleep {} && echo 'hi {}' > {}.txt", sleep, name, name);
        let root_input = Execution::new(name.to_string(), cli_cmd);
        dag.add_child(root_idx, i.try_into()?, DagrNode::Processor(root_input));
    }

    Ok((dag, root_idx))
}

enum DagrNode {
    List,
    Processor(Execution),
}

struct Execution {
    name: String,
    cli: String,
    workdir: String,
}

impl <'a> Execution {
    pub fn new(name: String, cli: String) -> Self {

        let container_workdir = Path::new(DATADIR).join(&name);
        let dir_string = container_workdir.to_string_lossy();

        Self {
            name,
            cli,
            workdir: dir_string.to_string(),
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

async fn process_graph() -> Result<(), Box<dyn std::error::Error>> {
    let (dag, idx) = graph().unwrap();
    let static_dag_ref: &'static DagrGraph<'static> = &'static dag;
    // for (_e, n) in dag.children(idx).iter(&dag) {
    //     match &dag[n] {
    //         DagrNode::Processor(execution) => {
    //             tokio::spawn(async move {
    //                 execute_container(execution)
    //             }).await.unwrap();
    //         },
    //         _ => unreachable!(),
    //     };
    // }
    Ok(())
}

async fn execute_container<'a>(execution: &'a Execution) -> DagrResult<'a> {
    // let container_workdir = Path::new(DATADIR).join(name.as_str());
    // let dir_string = container_workdir.to_string_lossy();

    let docker = Docker::connect_with_local_defaults()?;
    let opts = CreateContainerOptions {
        name: execution.name.as_str(),
    };

    let config = Config {
        cmd: Some(vec!["-c".to_string(), execution.cli.clone()]),
        entrypoint: Some(vec!["sh".to_string()]),
        host_config: Some(HostConfig{
            memory: Some(128 * 1024 * 1024),
            cpu_quota: Some(100_000),
            cpu_period: Some(100_000),
            binds: Some(vec![format!("{}:/workdir", execution.workdir)]),
            ..Default::default()
        }),
        image: Some("alpine:latest".to_string()),
        working_dir: Some("/workdir".to_string()),
        ..Default::default()
    };
    docker.create_container(Some(opts), config).await?;
    println!("Before start ({})", execution.name);
    docker.start_container(&execution.name, None::<StartContainerOptions<String>>).await?;
    println!("After start ({})", execution.name);
    let wait_opts = Some(WaitContainerOptions { condition: "not-running" });
    let responses = docker.wait_container(&execution.name, wait_opts).try_collect::<Vec<_>>().await?;

    let response = &responses[0];

    let result = DagrResultData {
        name: execution.name.clone(),
        exit_code: response.status_code,
        files: vec![DagrFile { path: execution.workdir.clone(), size: 0, checksum: 0 }],
    };

    Ok(result)
}

// async fn run_all() -> Result<(), DagrError> {
//     // let tasks = (0..10).map(|i| execute_container(&docker, i.to_string().as_str())).collect::<Vec<Future<Output=DagrResult>>>>();
//     let mut tasks = vec![];
//     for i in 0..10 {
//         // let name = i.to_string();
//         let task = tokio::spawn(async move {
//             let name = format!("container-{}", i);
//             let docker = Docker::connect_with_local_defaults().unwrap();
//             execute_container(&docker, name.to_string(), 5).await.unwrap()
//         });
//         tasks.push(task);
//     }

//     let task = tokio::spawn(async move {
//         let docker = Docker::connect_with_local_defaults().unwrap();
//         execute_container(&docker, "foo".to_string(), 1).await.unwrap()
//     });
//     tasks.push(task);

//     for task in tasks.drain(..) {
//         let result = task.await.unwrap();
//         println!("Container {} exit code: {} ({:?})", result.name, result.exit_code, result.files);
//     }  

//     println!("Done");
//     Ok(())
// }

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn it_works() {
        process_graph().await.unwrap();
    }

    #[test]
    fn test_dag() {
        graph().unwrap();
    }
}
