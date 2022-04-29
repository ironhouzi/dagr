use std::cell::Cell;
use std::fmt::Display;
use std::path::{Path, PathBuf};
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
// use daggy::petgraph::graph::node_index;
use daggy::{Dag, Walker};
// use futures_util::Future;

// use std::hash::Hash;
use futures_util::stream::TryStreamExt;
// use futures_util::future::join_all;
use minijinja::{Environment, Source, State};
use serde::Serialize;

use crate::error::DagrError;

// type DagrError = Box<dyn Error + 'static>;
const DATADIR: &str = "/tmp/dagr/data";
// const SLEEP: u32 = 4;

pub type DagrResult<'a> = Result<DagrData<'a>, DagrError>;
type DagrGraph = Dag::<DagrNode, Cell<DagrEdge>, u32>;
type DagrInput<'a> = HashMap<&'a str, DagrValue>;

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum DagrValue {
    Literal(String),
    Files(Vec<PathBuf>),
}

impl Display for DagrValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagrValue::Literal(literal) => write!(f, "{}", literal),
            DagrValue::Files(paths) => write!(f, "{}", paths
                .iter()
                .map(|path| format!("/inputs/{}", path.file_name().unwrap().to_string_lossy()))
                .collect::<Vec<String>>()
                .join(" ")
            ),
        }
    }
}

#[derive(Debug)]
pub enum DagrNode {
    List,
    Processor(Execution),
}

#[derive(Clone, Copy, Debug)]
pub enum DagrEdge {
    Resolved,
    Unresolved,
}

impl DagrEdge {
    pub fn new() -> Cell<Self> {
        Cell::new(DagrEdge::Unresolved)
    }
}

#[derive(Clone, Debug)]
pub struct Execution {
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
    path: PathBuf,
    bytes: usize,
    checksum: u64,
}

impl Display for DagrFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.to_string_lossy())
    }
}

#[derive(Debug)]
pub struct DagrData<'a> {
    name: &'a str,
    exit_code: i64,
    files: Vec<DagrFile>,
}

#[async_recursion(?Send)]
pub async fn process_graph(dag: &DagrGraph, idx: daggy::NodeIndex) -> DagrResult {
    let mut input: DagrInput = HashMap::new();

    for (e, n) in dag.children(idx).iter(dag) {
        if let Some(edge) = dag.edge_weight(e) {
            match edge.get() {
                DagrEdge::Unresolved => {
                    match &dag[n] {
                        DagrNode::Processor(_exec) => {
                            if let Some((_, child_idx)) = dag.edge_endpoints(e) {
                                if let Ok(data) = process_graph(dag, child_idx).await {
                                    input.insert(
                                        data.name,
                                        DagrValue::Files(
                                            data.files.iter().map(|f| f.path.clone()).collect()
                                        )
                                    );
                                    // Used simple solution of interior mutability, which should be
                                    // fine assuming docker containers are async and will not need
                                    // threads to handle execution.
                                    edge.set(DagrEdge::Resolved);
                                };
                            };
                        },
                        _ => unreachable!(),
                    };
                },
                DagrEdge::Resolved => continue
            }
        }
    }

    match &dag[idx] {
        DagrNode::Processor(exec) => {
            execute_container(exec, &input).await
        },
        _ => unreachable!(),
    }
}

// TODO: This function is useless unless it can convert PathBuf to container file paths
fn input_files(_state: &State, value: String) -> Result<String, minijinja::Error> {
    Ok(value)
}

async fn execute_container<'a, 'b>(execution: &'a Execution, input: &DagrInput<'b>) -> DagrResult<'a> {
    let docker = Docker::connect_with_local_defaults()?;
    let opts = CreateContainerOptions {
        name: execution.name.as_str(),
    };
    let mut env = Environment::new();
    let src = Source::new();
    env.set_source(src);
    env.add_template(&execution.name, &execution.cmd)?;
    env.add_function("fetch_files", input_files);
    // TODO: put PathBuf to String conversion in template function
    let mut container_input: HashMap<String, String> = HashMap::new();
    for (k, v) in input.iter() {
        match v {
            DagrValue::Files(_) => {
                container_input.insert(
                    k.to_string(),
                    v.to_string(),
                )
            },
            DagrValue::Literal(_) => container_input.insert(k.to_string(), v.to_string()),
        };

    }

    let cmd = env.get_template(&execution.name)?.render(container_input)?;
    let mut binds: Vec<String> = input
        .values()
        .filter_map(|val| match val {
            DagrValue::Files(files) => {
                Some(files
                    .iter()
                    .map(|file| format!(
                            "{}:/inputs/{}:ro",
                            file.to_string_lossy(),
                            file.file_name().unwrap().to_string_lossy()))
                    .collect::<Vec<String>>())
            },
            _ => None,
        })
        .flatten()
        .collect();

    binds.push(format!("{}:/workdir", execution.workdir));
    let config: Config<&str> = Config {
        cmd: Some(vec!["-c", &cmd]),
        entrypoint: Some(vec!["sh"]),
        host_config: Some(HostConfig{
            memory: Some(128 * 1024 * 1024),
            cpu_quota: Some(100_000),
            cpu_period: Some(100_000),
            binds: Some(binds),
            ..Default::default()
        }),
        image: Some("alpine:latest"),
        working_dir: Some("/workdir"),
        ..Default::default()
    };
    // TODO: logging
    println!("Create ({})", execution.name);
    docker.create_container(Some(opts), config).await?;
    println!("Await ({})", execution.name);
    docker.start_container(&execution.name, None::<StartContainerOptions<String>>).await?;
    let wait_opts = Some(WaitContainerOptions { condition: "not-running" });
    let responses = docker.wait_container(&execution.name, wait_opts).try_collect::<Vec<_>>().await?;
    println!("Done ({})", execution.name);

    let response = &responses[0];

    Ok(DagrData {
        name: &execution.name,
        exit_code: response.status_code,
        files: std::fs::read_dir(execution.workdir.clone())?
            .map(|dir| DagrFile { path: dir.unwrap().path(), bytes: 0, checksum: 0 } )
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::container::{RemoveContainerOptions, InspectContainerOptions};

    // From sebk (matrix):
    // ------------
    // a thread that takes Box<dyn Drop>
    // (via a channel)
    // sending things in a drop handler is a fairly low risk operation and then the other thread can deal with panics
    // ------------
    // It isn't a very elelegant solution, but it should work and not blow up in your face all the time because a drop handler paniced inside a panic handler
    // a global Reciever<Box<dyn Drop>>
    // then everything you want drop drop via that channel is wrapped in Options, so you can Option::take it in the &mut self drop handler
    // take the value, unwrap and Box it
    // then send it via the channel
    // the dedicated thread simply reads from the channel in a loop and does nothing else
    // just read and discard
    // or read and drop in catch_unwind, if your drop handlers panic a lot
    // ------------
    // REQUIRES TEST LIB TO START THREAD! :(

    #[tokio::test]
    async fn test_single_static_execution() -> Result<(), Box<dyn std::error::Error>> {
        let name = "test_static_exec";
        let static_execution = Execution::new(name.to_string(), "echo foo".to_string());
        let input = DagrInput::new();
        let result = execute_container(&static_execution, &input).await;
        match result {
            Ok(r) => assert_eq!(r.exit_code, 0),
            Err(e) => {
                cleanup(vec!(name.to_owned())).await?;
                panic!("{e}")
            },
        };
        cleanup(vec!(name.to_owned())).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_single_dynamic_literal() -> Result<(), Box<dyn std::error::Error>> {
        let name = "test_dynamic_exec";
        let dynamic_execution = Execution::new(name.to_string(), "echo {{ literal_val }}".to_string());
        let mut input = DagrInput::new();
        input.insert("literal_val", DagrValue::Literal("dynamic".to_string()));
        let result = execute_container(&dynamic_execution, &input).await;
        let docker = Docker::connect_with_local_defaults().unwrap();
        let info = docker.inspect_container(name, Some(InspectContainerOptions{..Default::default()})).await.unwrap();
        assert_eq!(info.args, Some(vec!["-c".to_string(), "echo dynamic".to_string()]));
        // TODO: learn streams
        // docker.logs(name, Some(LogsOptions { stdout: true, ..Default::default() })).try_collect();
        match result {
            Ok(r) => {
                assert_eq!(r.exit_code, 0);
            },
            Err(e) => {
                cleanup(vec!(name.to_owned())).await?;
                panic!("{e}")
            },
        };
        cleanup(vec!(name.to_owned())).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_single_static_file_output() -> Result<(), Box<dyn std::error::Error>> {
        let name = "test_static_file_output";
        let execution = Execution::new(name.to_string(), "echo foo > output.txt".to_string());
        let input = DagrInput::new();
        let result = execute_container(&execution, &input).await;
        let verification = match result {
            Ok(r) => {
                r.exit_code.eq(&0) &&
                r.files[0].path.eq(&PathBuf::from(format!("{}/output.txt", execution.workdir)))
            },
            Err(e) => {
                cleanup(vec!(name.to_owned())).await?;
                panic!("{e}")
            },
        };
        cleanup(vec!(name.to_owned())).await?;
        assert!(verification);
        Ok(())
    }

    #[tokio::test]
    async fn test_chained() -> Result<(), Box<dyn std::error::Error>> {
        fn chain_graph() -> Result<(DagrGraph, daggy::NodeIndex), Box<dyn std::error::Error>> {
            let mut dag = DagrGraph::new();

            let name = "chain_last";
            let i_input = Execution::new(name.to_string(), "echo \"hello $(cat {{ fetch_files(chain_first) }})\" > foo.txt".to_string());
            let root_idx = dag.add_node(DagrNode::Processor(i_input));

            let name = "chain_first";
            let inner_cmd = "echo 'world' > foo.txt";
            let input = Execution::new(name.to_string(), inner_cmd.to_string());
            dag.add_child(root_idx, DagrEdge::new(), DagrNode::Processor(input));

            Ok((dag, root_idx))
        }

        let (dag, root_idx) = chain_graph().unwrap();
        let result = process_graph(&dag, root_idx).await;
        let verification = verify_result(result, "hello world\n").unwrap_or(false);
        cleanup(dag_containers(&dag)?).await?;
        assert!(verification);
        Ok(())
    }

    #[tokio::test]
    async fn test_multifile_chain() -> Result<(), Box<dyn std::error::Error>> {
        fn chain_graph() -> Result<(DagrGraph, daggy::NodeIndex), Box<dyn std::error::Error>> {
            let mut dag = DagrGraph::new();

            let name = "chain2_last";
            let i_input = Execution::new(name.to_string(), "cat {{ fetch_files(chain2_first) }} > foo.txt".to_string());
            let root_idx = dag.add_node(DagrNode::Processor(i_input));

            let name = "chain2_first";
            let inner_cmd = "echo 'hello' > hello.txt; echo 'world' > world.txt;";
            // let inner_cmd = "echo world > world.txt; echo hello > hello.txt;";
            let input = Execution::new(name.to_string(), inner_cmd.to_string());
            dag.add_child(root_idx, DagrEdge::new(), DagrNode::Processor(input));

            Ok((dag, root_idx))
        }

        let (dag, root_idx) = chain_graph().unwrap();
        let result = process_graph(&dag, root_idx).await;
        let verification = verify_result(result, "world\nhello\n").unwrap_or(false);
        cleanup(dag_containers(&dag)?).await?;
        assert!(verification);
        Ok(())
    }

    fn verify_result(result: DagrResult, valid_file_content: &str) -> Result<bool, DagrError> {
        let r = result?;
        let result_file = &r.files[0].path;
        assert_eq!(r.exit_code, 0);
        assert_eq!(result_file, &PathBuf::from(format!("{}/{}/foo.txt", DATADIR, r.name)));
        let file_content = std::fs::read_to_string(result_file)?;
        assert_eq!(file_content, valid_file_content);
        Ok(true)
    }

    async fn clean_datadir(container_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let cleanup_container = format!("cleanup_{}", container_name);
        let docker = Docker::connect_with_local_defaults()?;
        let opts = CreateContainerOptions {
            name: &cleanup_container,
        };
        let cmd = format!("rm -rf {}", container_name);
        let config: Config<&str> = Config {
            cmd: Some(vec!["-c", &cmd]),
            entrypoint: Some(vec!["sh"]),
            host_config: Some(HostConfig{
                auto_remove: Some(true),
                cpu_quota: Some(100_000),
                cpu_period: Some(100_000),
                binds: Some(vec!(format!("{}:/data", DATADIR))),
                memory: Some(128 * 1024 * 1024),
                ..Default::default()
            }),
            image: Some("alpine:latest"),
            working_dir: Some("/data"),
            ..Default::default()
        };
        docker.create_container(Some(opts), config).await?;
        docker.start_container(&cleanup_container, None::<StartContainerOptions<String>>).await?;
        let wait_opts = Some(WaitContainerOptions { condition: "not-running" });
        docker.wait_container(&cleanup_container, wait_opts).try_collect::<Vec<_>>().await?;
        Ok(())
    }

    fn dag_containers(dag: &DagrGraph) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(dag
           .raw_nodes()
           .iter()
           .filter_map(|node| match &node.weight {
            DagrNode::Processor(execution) => Some(execution.name.clone()),
            _ => None,
           })
           .collect())
    }

    async fn clean_containers(container_names: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        let docker = Docker::connect_with_local_defaults()?;
        let opts = Some(RemoveContainerOptions{ force: true, ..Default::default()});
        for container_name in container_names {
            docker.remove_container(container_name, opts).await?;
        }

        Ok(())
    }

    async fn cleanup(container_names: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        clean_containers(&container_names).await?;
        for container_name in container_names {
            clean_datadir(&container_name).await?;
        }
        Ok(())
    }
}
