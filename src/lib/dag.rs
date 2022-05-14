#![allow(dead_code)]

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
    LogOutput,
    LogsOptions,
    StartContainerOptions,
    WaitContainerOptions,
};
use bollard::models::HostConfig;
use daggy::{Dag, Walker};

use futures_util::stream::TryStreamExt;
use minijinja::{Environment, Source, State};
use serde::Serialize;
use tokio_stream::StreamExt;

use crate::error::DagrError;

const DATADIR: &str = "/tmp/dagr/data";

pub type GraphResult<'a> = Result<GraphOutput<'a>, DagrError>;
type DagrGraph = Dag::<DagrNode, DagrEdge, u32>;
type DagrInput<'a> = HashMap<&'a str, Vec<DagrValue>>;

pub enum GraphOutput<'a> {
    Execution(ExecutionOutput<'a>),
    List((&'a str, Vec<DagrValue>)),
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum DagrValue {
    Literal(String),
    File(PathBuf),
}

impl Display for DagrValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagrValue::Literal(literal) => write!(f, "{}", literal),
            DagrValue::File(path) => write!(
                f,
                "/inputs/{}",
                path.file_name().unwrap().to_string_lossy(),
            ),
        }
    }
}

impl DagrValue {
    fn bind_mount(&self) -> String {
        match &self {
            DagrValue::Literal(s) => s.to_string(),
            DagrValue::File(path) => format!(
                "{}:{}:ro",
                path.to_string_lossy(),
                &self.to_string(),
            ),
        }
    }
}

#[derive(Debug)]
pub enum DagrNode {
    List(String),
    Processor(ExecutionInput),
}

pub fn new_list_node(name: String, dag: &mut DagrGraph, list_items: Vec<DagrNode>) -> daggy::NodeIndex {
    let this_node_index = dag.add_node(DagrNode::List(name));
    for (counter, list_element_node) in list_items.into_iter().enumerate() {
        let edge = DagrEdge::new(Some(EdgeData{ key: None, value: counter.to_string() }));
        dag.add_child(this_node_index, edge, list_element_node);
    }

    this_node_index
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum EdgeStatus {
    Resolved,
    Unresolved,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgeData {
    key: Option<String>,
    value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DagrEdge {
    // Used simple solution of interior mutability, which should be fine assuming docker containers
    // are async and will not traverse thread boundaries to handle execution.
    status: Cell<EdgeStatus>,
    data: Option<EdgeData>,
}

impl DagrEdge {
    pub fn new(data: Option<EdgeData>) -> Self {
        Self { status: Cell::new(EdgeStatus::Unresolved), data }
    }
}

impl Default for DagrEdge {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionInput {
    cmd: String,
    name: String,
    workdir: String,
}

impl ExecutionInput {
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

pub struct ExecutionOutput<'a> {
    name: &'a str,
    exit_code: i64,
    files: Vec<DagrFile>,
}

#[async_recursion(?Send)]
pub async fn process_graph(dag: &DagrGraph, node_index: daggy::NodeIndex) -> GraphResult {
    let mut input: DagrInput = HashMap::new();
    let mut ordered_list_input: Vec<(usize, &str)> = Vec::new();
    let is_ordered = matches!(&dag[node_index], DagrNode::List(_));

    for (child_edge_index, child_node_index) in dag.children(node_index).iter(dag) {
        let edge = match dag.edge_weight(child_edge_index) {
            Some(e) => e,
            None => continue,
        };

        if edge.status.get() == EdgeStatus::Resolved {
            continue
        }

        match process_graph(dag, child_node_index).await? {
            GraphOutput::Execution(result) => {
                input.insert(
                    result.name,
                    result.files
                        .iter()
                        .map(|f| DagrValue::File(f.path.clone()))
                        .collect()
                );
                if is_ordered {
                    let edge_data = &edge.data;
                    let order = edge_data
                        .as_ref()
                        .map_or(usize::MAX, |data| data.value.parse::<usize>().unwrap_or(usize::MAX));

                    ordered_list_input.push((order, result.name));
                }
            },
            GraphOutput::List((name, list)) => {
                input.insert(name, list);
            }
        }
        // TODO: Determine edge state from ExecutionOutput.exit_code
        edge.status.set(EdgeStatus::Resolved);
    }

    match &dag[node_index] {
        DagrNode::Processor(exec) => {
            execute_container(exec, &input).await
        },
        DagrNode::List(list_name) => {
            let mut sorted_input: Vec<DagrValue> = Vec::new();
            ordered_list_input.sort_unstable();
            for (_, key) in ordered_list_input {
                sorted_input.extend_from_slice(&input[&key]);
            }
            Ok(GraphOutput::List((list_name, sorted_input)))
        },
    }
}

// TODO: This function is useless unless it can convert PathBuf to container file paths
fn input_files(_state: &State, value: String) -> Result<String, minijinja::Error> {
    Ok(value)
}

async fn execute_container<'a, 'b>(execution: &'a ExecutionInput, input: &DagrInput<'b>) -> GraphResult<'a> {
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
    let mut template_input: HashMap<String, String> = HashMap::new();
    for (key, output_values) in input.iter() {
        template_input.insert(
            key.to_string(),
            output_values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(" "));
    }

    let cmd = env.get_template(&execution.name)?.render(template_input)?;
    let mut binds: Vec<String> = input
        .values()
        .flat_map(|output_values| output_values
                  .iter()
                  .filter_map(|output_value| match output_value {
                      DagrValue::File(_) => Some(output_value.bind_mount()),
                      _ => None,
                  }))
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
    let responses = docker
        .wait_container(&execution.name, wait_opts)
        .try_collect::<Vec<_>>()
        .await?;

    let log_options = Some(LogsOptions::<String> {
        stdout: true,
        ..Default::default()
    });
    let mut log_stream = docker.logs(&execution.name, log_options);
    while let Some(log_entry) = log_stream.next().await {
        match log_entry? {
            LogOutput::StdOut{message} => {
                if let Ok(valid_utf8) = String::from_utf8(message.to_vec()) {
                    // TODO: format container log to distinguish from application log
                    println!("    -- Container stdout -- {}",
                             valid_utf8.strip_suffix('\n').unwrap_or(&valid_utf8))
                } else {
                    continue
                }
            },
            // TODO: stderr
            _ => continue,
        }
    }

    println!("Done ({})", execution.name);
    let response = &responses[0];

    Ok(GraphOutput::Execution(ExecutionOutput {
        name: &execution.name,
        exit_code: response.status_code,
        files: std::fs::read_dir(execution.workdir.clone())?
            .map(|dir| DagrFile { path: dir.unwrap().path(), bytes: 0, checksum: 0 } )
            .collect(),
    }))
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
        let static_execution = ExecutionInput::new(name.to_string(), "echo foo".to_string());
        let input = DagrInput::new();
        let result = execute_container(&static_execution, &input).await;
        match result {
            Ok(r) => {
                match r {
                    GraphOutput::List(_) => {
                        cleanup(vec!(name.to_owned())).await?;
                        panic!("Expected execution output, got List!")
                    },
                    GraphOutput::Execution(output) => assert_eq!(output.exit_code, 0),
                };
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
    async fn test_single_dynamic_literal() -> Result<(), Box<dyn std::error::Error>> {
        let name = "test_dynamic_exec";
        let dynamic_execution = ExecutionInput::new(name.to_string(), "echo {{ literal_val }}".to_string());
        let mut input = DagrInput::new();
        input.insert("literal_val", vec!(DagrValue::Literal("dynamic".to_string())));
        let result = execute_container(&dynamic_execution, &input).await;
        // TODO: learn streams
        match result {
            Ok(r) => {
                let docker = Docker::connect_with_local_defaults().unwrap();
                let info = docker.inspect_container(name, Some(InspectContainerOptions{..Default::default()})).await.unwrap();
                assert_eq!(info.args, Some(vec!["-c".to_string(), "echo dynamic".to_string()]));
                match r {
                    GraphOutput::List(_) => {
                        cleanup(vec!(name.to_owned())).await?;
                        panic!("Expected execution output, got List!")
                    },
                    GraphOutput::Execution(output) => assert_eq!(output.exit_code, 0),
                };
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
        let execution = ExecutionInput::new(name.to_string(), "echo foo > output.txt".to_string());
        let input = DagrInput::new();
        let result = execute_container(&execution, &input).await;
        let verification = match result {
            Ok(r) => {
                match r {
                    GraphOutput::List(_) => false,
                    GraphOutput::Execution(exec) => {
                        exec.exit_code.eq(&0) &&
                            exec.files[0].path.eq(&PathBuf::from(format!("{}/output.txt", execution.workdir)))
                    }
                }
            },
            Err(_) => {
                cleanup(vec!(name.to_owned())).await?;
                false
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
            let i_input = ExecutionInput::new(name.to_string(), "echo \"hello $(cat {{ fetch_files(chain_first) }})\" > result.txt".to_string());
            let root_idx = dag.add_node(DagrNode::Processor(i_input));

            let name = "chain_first";
            let inner_cmd = "echo 'world' > result.txt";
            let input = ExecutionInput::new(name.to_string(), inner_cmd.to_string());
            dag.add_child(root_idx, DagrEdge::default(), DagrNode::Processor(input));

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
            let i_input = ExecutionInput::new(name.to_string(), "cat {{ fetch_files(chain2_first) }} > result.txt".to_string());
            let root_idx = dag.add_node(DagrNode::Processor(i_input));

            let name = "chain2_first";
            let inner_cmd = "echo 'hello' > hello.txt; echo 'world' > world.txt;";
            // let inner_cmd = "echo world > world.txt; echo hello > hello.txt;";
            let input = ExecutionInput::new(name.to_string(), inner_cmd.to_string());
            dag.add_child(root_idx, DagrEdge::default(), DagrNode::Processor(input));

            Ok((dag, root_idx))
        }

        let (dag, root_idx) = chain_graph().unwrap();
        let result = process_graph(&dag, root_idx).await;
        let verification = verify_result(result, "world\nhello\n").unwrap_or(false);
        cleanup(dag_containers(&dag)?).await?;
        assert!(verification);
        Ok(())
    }

    #[tokio::test]
    async fn test_multinode_chain() -> Result<(), Box<dyn std::error::Error>> {
        fn chain_graph() -> Result<(DagrGraph, daggy::NodeIndex), Box<dyn std::error::Error>> {
            let mut dag = DagrGraph::new();

            let name = "chain3_root";
            // TODO: write test case that ensures a descriptive error is produced from this case:
            // let cmd = "cat {{ fetch_files(chain3_child1) fetch_files(chain3_child2) }} > result.txt".to_string();
            let cmd = "cat {{ fetch_files(chain3_child1) }} {{ fetch_files(chain3_child2) }} > result.txt".to_string();
            let i_input = ExecutionInput::new(name.to_string(), cmd);
            let root_idx = dag.add_node(DagrNode::Processor(i_input));

            let name = "chain3_child1";
            let inner_cmd = "echo 'hello' > hello.txt";
            let input = ExecutionInput::new(name.to_string(), inner_cmd.to_string());
            dag.add_child(root_idx, DagrEdge::default(), DagrNode::Processor(input));

            let name = "chain3_child2";
            let inner_cmd = "echo 'world' > world.txt";
            let input = ExecutionInput::new(name.to_string(), inner_cmd.to_string());
            dag.add_child(root_idx, DagrEdge::default(), DagrNode::Processor(input));

            Ok((dag, root_idx))
        }

        let (dag, root_idx) = chain_graph().unwrap();
        let result = process_graph(&dag, root_idx).await;
        let verification = verify_result(result, "hello\nworld\n").unwrap_or(false);
        cleanup(dag_containers(&dag)?).await?;
        assert!(verification);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_node() -> Result<(), Box<dyn std::error::Error>> {
        fn graph() -> Result<(DagrGraph, daggy::NodeIndex), Box<dyn std::error::Error>> {
            let mut dag = DagrGraph::new();

            let name = "list_element_1";
            let inner_cmd = "echo 'hello' > hello.txt";
            let input1 = ExecutionInput::new(name.to_string(), inner_cmd.to_string());

            let name = "list_element_2";
            let inner_cmd = "echo 'world' > world.txt";
            let input2 = ExecutionInput::new(name.to_string(), inner_cmd.to_string());

            let list_node_idx = new_list_node(
                "results".to_string(),
                &mut dag,
                vec![DagrNode::Processor(input1), DagrNode::Processor(input2)]);

            let name = "root";
            let outer_cmd = "cat {{ fetch_files(results) }} > result.txt";
            let input = ExecutionInput::new(name.to_string(), outer_cmd.to_string());
            let root_idx = dag.add_node(DagrNode::Processor(input));

            dag.add_edge(root_idx, list_node_idx, DagrEdge::default())?;
            Ok((dag, root_idx))
        }
        let (dag, root_idx) = graph().unwrap();
        let result = process_graph(&dag, root_idx).await;
        let verification = verify_result(result, "hello\nworld\n").unwrap_or(false);
        cleanup(dag_containers(&dag)?).await?;
        assert!(verification);
        Ok(())
    }

    fn verify_result(result: GraphResult, valid_file_content: &str) -> Result<bool, DagrError> {
        match result? {
            GraphOutput::Execution(r) => {
                let result_file = &r.files[0].path;
                assert_eq!(r.exit_code, 0);
                assert_eq!(result_file, &PathBuf::from(format!("{}/{}/result.txt", DATADIR, r.name)));
                let file_content = std::fs::read_to_string(result_file)?;
                assert_eq!(file_content, valid_file_content);
            },
            GraphOutput::List(_) => unimplemented!(),
        }
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
