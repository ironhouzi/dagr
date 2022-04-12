use std::fmt::Display;
use std::path::Path;
use std::string::String;
use std::collections::HashMap;
use std::default::Default;

use async_recursion::async_recursion;
use bollard::Docker;
use bollard::container::{
    Config,
    CreateContainerOptions,
    // LogsOptions,
    StartContainerOptions,
    WaitContainerOptions,
};
use bollard::models::HostConfig;
use daggy::petgraph::graph::node_index;
use daggy::{Dag, Walker};
// use futures_util::Future;
// use tokio::fs;

// use std::hash::Hash;
use futures_util::stream::TryStreamExt;
// use futures_util::future::join_all;
use minijinja::{Environment, Source, State};
use serde::Serialize;

use crate::error::DagrError;

// type DagrError = Box<dyn Error + 'static>;
const DATADIR: &str = "/tmp/dagr/data";
const SLEEP: u32 = 4;

pub type DagrResult<'a> = Result<DagrData<'a>, DagrError>;
type DagrGraph = Dag::<DagrNode, DagrEdge, u32>;
type DagrInput<'a> = HashMap<&'a str, DagrValue>;

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum DagrValue {
    Literal(String),
    Files(Vec<String>),
}

#[derive(Debug)]
enum DagrNode {
    List,
    Processor(Execution),
}

#[derive(Debug)]
enum DagrEdge {
    Resolved(daggy::NodeIndex),
    Unresolved,
}

#[derive(Clone, Debug)]
struct Execution {
    cmd: String,
    name: String,
    workdir: String,
}

impl Execution {
    pub fn new(name: String, cmd: String) -> Self {

        let container_workdir = Path::new(DATADIR).join(&name);
        let dir_string = container_workdir.to_string_lossy();

        Self {
            cmd,
            name,
            workdir: dir_string.to_string(),
        }
    }
}

#[derive(Debug)]
struct DagrFile {
    path: String,
    bytes: usize,
    checksum: u64,
}

impl Display for DagrFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

#[derive(Debug)]
pub struct DagrData<'a> {
    name: &'a str,
    exit_code: i64,
    files: Vec<DagrFile>,
}



// fn sibling_graph() -> Result<(DagrGraph, daggy::NodeIndex), Box<dyn std::error::Error>> {
//     let mut dag = DagrGraph::new();
//     let names = vec!["foo", "bar", "baz"];
//     let sleep = 4;
//     let root_idx = dag.add_node(DagrNode::List);

//     for name in names.iter() {
//         let cli_cmd = format!("sleep {} && echo 'hi {}' > {}.txt", sleep, name, name);
//         let input = Execution::new(name.to_string(), Some(cli_cmd));
//         let edge = DagrEdge::Unresolved;
//         dag.add_child(root_idx, edge, DagrNode::Processor(input));
//     }

//     Ok((dag, root_idx))
// }

fn chain_graph() -> Result<(DagrGraph, daggy::NodeIndex), Box<dyn std::error::Error>> {
    let mut dag = DagrGraph::new();

    // let names = vec!["foo", "bar", "baz"];
    // let root_idx = dag.add_node(DagrNode::List);

    let name = "last";
    // let inner_cmd = format!("", SLEEP, name);
    let i_input = Execution::new(name.to_string(), "wc -l {{ fetch_files(first) }} > foo.txt".to_string());
    let root_idx = dag.add_node(DagrNode::Processor(i_input));

    let name = "first";
    let inner_cmd = "echo \"world\" > foo.txt";
    let input = Execution::new(name.to_string(), inner_cmd.to_string());
    let edge = DagrEdge::Unresolved;
    dag.add_child(root_idx, edge, DagrNode::Processor(input));

    Ok((dag, root_idx))
}

#[async_recursion(?Send)]
async fn process_graph(dag: &mut DagrGraph, idx: daggy::NodeIndex) -> DagrResult {
    let mut input: DagrInput = HashMap::new();
    for (e, n) in dag.children(idx).iter(dag) {
        match dag.edge_weight(e) {
            Some(DagrEdge::Unresolved) => {
                match &mut dag[n] {
                    DagrNode::Processor(_exec) => {
                        if let Some((parent_idx, child_idx)) = dag.edge_endpoints(e) {
                            if let Ok(data) = process_graph(dag, child_idx).await {
                                input.insert(
                                    data.name,
                                    DagrValue::Files(
                                        data.files.iter()
                                        .map(|f| f.to_string())
                                        .collect()
                                    )
                                );
                                dag.update_edge(parent_idx, child_idx, DagrEdge::Resolved(child_idx));
                            };
                        };

                    },
                    _ => unreachable!(),
                };
            },
            _ => continue
        }
    }
    match &dag[idx] {
        DagrNode::Processor(exec) => {
            execute_container(exec, &input).await
        },
        _ => unreachable!(),
    }
}

fn input_files(_state: &State, value: Vec<String>) -> Result<String, minijinja::Error> {
    // TODO: ensure sting quotes are properly escaped.
    Ok(value.join(" "))
}

async fn execute_container<'a, 'b>(execution: &'a Execution, input: &DagrInput<'b>) -> DagrResult<'a> {
    
    // let container_workdir = Path::new(DATADIR).join(name.as_str());
    // let dir_string = container_workdir.to_string_lossy();

    let docker = Docker::connect_with_local_defaults()?;
    let opts = CreateContainerOptions {
        name: execution.name.as_str(),
    };
    let mut env = Environment::new();
    let src = Source::new();
    env.set_source(src);
    env.add_template(&execution.name, &execution.cmd)?;
    env.add_function("fetch_files", input_files);
    // env.set_source(Source::with_loader(|name| {
    //     if name == "test_static_exec"
    //     Ok(Some(execution.cmd))
    // }));
    let cmd = env.get_template(&execution.name)?.render(input)?;

    let config = Config {
        cmd: Some(vec!["-c".to_string(), cmd]),
        // cmd: Some(vec!["-c".to_string(), execution.cli.clone()]),
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
    println!("Done ({})", execution.name);

    let response = &responses[0];

    Ok(DagrData {
        name: &execution.name,
        exit_code: response.status_code,
        files: std::fs::read_dir(execution.workdir.clone())?
            .map(|res| res.map(|dir| dir.path().to_string_lossy().to_string()))
            .map(|dir| DagrFile { path: dir.unwrap(), bytes: 0, checksum: 0 } )
            .collect(),
    })
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

// async fn recurse_graph() -> Result<(), Box<dyn std::error::Error>> {
//     let (dag, idx) = chain_graph().unwrap();
//     let mut executions: Vec<&Execution> = vec!();
//     for (_e, n) in dag.children(idx).iter(&dag) {
//         match &dag[n] {
//             DagrNode::Processor(exec) => {
//                 executions.push(exec)
//             },
//             _ => unreachable!(),
//         };
//     }
//     Ok(())
// }

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::container::{RemoveContainerOptions, InspectContainerOptions};

    // #[tokio::test]
    // async fn test_single_static_execution() {
    //     let name = "test_static_exec";
    //     let static_execution = Execution::new(name.to_string(), "echo foo".to_string());
    //     let input = DagrInput::new();
    //     let result = execute_container(&static_execution, input).await;
    //     let docker = Docker::connect_with_local_defaults().unwrap();
    //     docker.remove_container(name, Some(RemoveContainerOptions{ force: true, ..Default::default()})).await.unwrap();
    //     match result {
    //         Ok(r) => assert_eq!(r.exit_code, 0),
    //         Err(e) => panic!("{e}"),
    //     };
    // }

    // #[tokio::test]
    // async fn test_single_dynamic_execution() {
    //     let name = "test_dynamic_exec";
    //     let dynamic_execution = Execution::new(name.to_string(), "echo {{ name }}".to_string());
    //     let mut input = DagrInput::new();
    //     input.insert("name", DagrValue::Literal("dynamic".to_string()));
    //     let result = execute_container(&dynamic_execution, input).await;
    //     let docker = Docker::connect_with_local_defaults().unwrap();
    //     let info = docker.inspect_container(name, Some(InspectContainerOptions{..Default::default()})).await.unwrap();
    //     assert_eq!(info.args, Some(vec!["-c".to_string(), "echo dynamic".to_string()]));
    //     // TODO: learn streams
    //     // docker.logs(name, Some(LogsOptions { stdout: true, ..Default::default() })).try_collect();
    //     docker.remove_container(name, Some(RemoveContainerOptions{ force: true, ..Default::default()})).await.unwrap();
    //     match result {
    //         Ok(r) => {
    //             assert_eq!(r.exit_code, 0);
    //         },
    //         Err(e) => panic!("{e}"),
    //     };
    // }

    // #[tokio::test]
    // async fn test_dynamic_files() {
    //     let name = "test_dynamic_file";
    //     let dynamic_execution = Execution::new(name.to_string(), "echo \"{{ fetch_files(files) }}\"".to_string());
    //     let mut input = DagrInput::new();
    //     input.insert("files", DagrValue::Files(["foo", "bar"].into_iter().map(ToOwned::to_owned).collect()));
    //     let result = execute_container(&dynamic_execution, input).await;
    //     let docker = Docker::connect_with_local_defaults().unwrap();
    //     let info = docker.inspect_container(name, Some(InspectContainerOptions{..Default::default()})).await.unwrap();
    //     assert_eq!(info.args, Some(vec!["-c".to_string(), "echo \"foo bar\"".to_string()]));
    //     // TODO: learn streams
    //     // docker.logs(name, Some(LogsOptions { stdout: true, ..Default::default() })).try_collect();
    //     docker.remove_container(name, Some(RemoveContainerOptions{ force: true, ..Default::default()})).await.unwrap();
    //     match result {
    //         Ok(r) => {
    //             assert_eq!(r.exit_code, 0);
    //         },
    //         Err(e) => panic!("{e:?}"),
    //     };
    // }

    // #[tokio::test]
    // async fn test_single_static_file_output() {
    //     let name = "test_static_file_output";
    //     let execution = Execution::new(name.to_string(), "echo foo > output.txt".to_string());
    //     let mut input = DagrInput::new();
    //     let result = execute_container(&execution, input).await;
    //     let docker = Docker::connect_with_local_defaults().unwrap();
    //     docker.remove_container(name, Some(RemoveContainerOptions{ force: true, ..Default::default()})).await.unwrap();
    //     match result {
    //         Ok(r) => {
    //             assert_eq!(r.exit_code, 0);
    //             assert_eq!(r.files[0].path, format!("{}/output.txt", execution.workdir));
    //         },
    //         Err(e) => panic!("{e}"),
    //     };
    // }

    #[tokio::test]
    async fn test_chained() {
        let (mut dag, root_idx) = chain_graph().unwrap();
        process_graph(&mut dag, root_idx).await.unwrap();
    }

    // #[test]
    // fn test_dag() {
    //     graph().unwrap();
    // }
}
