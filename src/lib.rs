use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr},
    sync::Mutex,
    time::{Duration, Instant},
};

use bollard::{
    container::{
        Config, CreateContainerOptions, ListContainersOptions, LogOutput, StartContainerOptions,
    },
    exec::{CreateExecOptions, StartExecResults},
    image::CreateImageOptions,
    models::HostConfig,
    Docker,
};
use derive_more::{Display, Error, From};
use futures::StreamExt;
use once_cell::sync::Lazy;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::sleep};
use tracing::{debug, error, info, warn};

pub mod clickhouse;

/// Special application that cleans up docker resources
static CLEANUP_IMG: &str = "testcontainers/ryuk:0.3.2";

/// Label of resources to clean up
static TEST_CONTAINER_LABEL: &str = "io.github.injecto.rusty-tub.test";
static TEST_CONTAINER_LABEL_VALUE: &str = "1";

static MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Debug, Display, Error, From)]
pub enum Error {
    #[display(fmt = "docker interaction error")]
    Docker(bollard::errors::Error),
    #[display(fmt = "cleaning container setup error")]
    Cleaner(std::io::Error),
    Inspection {
        message: String,
    },
    #[display(fmt = "timeout while trying connecting to container's port")]
    WaitForPortTimeout,
}

pub async fn up(image: &str, expose_ports: &[u16]) -> Result<Container, Error> {
    // eliminate races around docker containers state
    let _guard = MUTEX.lock().unwrap();

    let docker = Docker::connect_with_local_defaults()?;
    pull(&docker, image).await?;
    let mut labels = HashMap::new();
    labels.insert(
        TEST_CONTAINER_LABEL.to_owned(),
        TEST_CONTAINER_LABEL_VALUE.to_owned(),
    );
    let mut container = run(&docker, image, expose_ports, None, Some(labels)).await?;

    // TODO don't clean up if a test panics
    if env::var("LEAVE_CONTAINER").map_or(true, |v| v != "1") {
        let flag = defer_cleanup(
            &docker,
            &format!("{}={}", TEST_CONTAINER_LABEL, TEST_CONTAINER_LABEL_VALUE),
        )
        .await?;
        // container should hold the flag to prevent cleaning up
        container.alive_flag = Some(flag);
    }
    Ok(container)
}

/// Defer clean up of [`label`]ed resources after [`AliveFlag`] is gone
async fn defer_cleanup(docker: &Docker, label: &str) -> Result<AliveFlag, Error> {
    pull(docker, CLEANUP_IMG).await?;
    let port = 8080;
    let cleaner = run(
        docker,
        CLEANUP_IMG,
        &[port],
        Some(vec!["/var/run/docker.sock:/var/run/docker.sock".to_owned()]),
        None,
    )
    .await?;
    cleaner
        .wait_for_port_listening(port, Duration::from_secs(5))
        .await?;
    let ip = cleaner.ip().await?;
    let addr = SocketAddr::new(ip, port);

    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("label={}", label);
    stream.write_all(request.as_bytes()).await?;
    debug!(request = ?request, "ryuk setup");

    Ok(AliveFlag(stream))
}

/// Pull fresh image version or do nothing
async fn pull(docker: &Docker, image: &str) -> Result<(), Error> {
    let opts = CreateImageOptions {
        from_image: image,
        ..Default::default()
    };
    let mut pull_infos = docker.create_image(Some(opts), None, None);
    while let Some(pull_res) = pull_infos.next().await {
        let info = pull_res?;
        let progress_current = info.progress_detail.as_ref().and_then(|d| d.current);
        let progress_total = info.progress_detail.as_ref().and_then(|d| d.total);
        let progress = progress_current
            .zip(progress_total)
            .map(|(c, t)| (c as f64 / t as f64 * 100.0).ceil());
        if let Some(e) = info.error {
            error!(
                pull_id = ?info.id,
                error = ?e,
                status = ?info.status,
                progress = progress,
                "pull",
            );
        } else {
            info!(
                pull_id = ?info.id,
                status = ?info.status,
                progress = progress,
                "pull",
            );
        }
    }

    Ok(())
}

async fn run(
    docker: &Docker,
    image: &str,
    expose_ports: &[u16],
    volume_bindings: Option<Vec<String>>,
    labels: Option<HashMap<String, String>>,
) -> Result<Container, Error> {
    let image_info = docker.inspect_image(image).await?;

    let mut container_filters = HashMap::new();
    container_filters.insert("ancestor", vec![image_info.id.as_str()]);

    // check an existed containers built from the image
    let available_containers = docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: container_filters,
            ..Default::default()
        }))
        .await?;

    let running_opt = available_containers
        .iter()
        .find(|c| c.state.as_ref().map(|s| s == "running").unwrap_or_default());
    if let Some(running) = running_opt {
        info!(container = ?running, "using running container");
        return Ok(Container::new(
            docker.clone(),
            running.id.as_ref().expect("has id").clone(),
        ));
    }

    let runnable_opt = available_containers.iter().find(|c| {
        c.state
            .as_ref()
            .map(|s| s == "created" || s == "exited")
            .unwrap_or_default()
    });
    if let Some(runnable) = runnable_opt {
        info!(container = ?runnable, "starting existed container");
        docker
            .start_container(
                runnable.id.as_ref().expect("has id"),
                None::<StartContainerOptions<String>>,
            )
            .await?;
        return Ok(Container::new(
            docker.clone(),
            runnable.id.as_ref().expect("has id").clone(),
        ));
    }

    // lets create a new container

    let exposed_ports = expose_ports
        .iter()
        .map(|p| (p.to_string(), HashMap::new()))
        .collect();

    let host_cfg = HostConfig {
        binds: volume_bindings,
        ..Default::default()
    };

    let created_info = docker
        .create_container(
            None::<CreateContainerOptions<&str>>,
            Config {
                image: Some(image.to_owned()),
                exposed_ports: Some(exposed_ports),
                host_config: Some(host_cfg),
                labels,
                ..Default::default()
            },
        )
        .await?;
    info!(id = ?created_info.id, "container created");

    docker
        .start_container(&created_info.id, None::<StartContainerOptions<String>>)
        .await?;
    info!(id = ?created_info.id, "container started");

    Ok(Container::new(docker.clone(), created_info.id))
}

#[derive(Debug)]
pub struct Container {
    id: String,
    docker: Docker,
    alive_flag: Option<AliveFlag>,
}

impl Container {
    fn new(docker: Docker, id: String) -> Self {
        Self {
            docker,
            id,
            alive_flag: None,
        }
    }

    pub async fn ip(&self) -> Result<IpAddr, Error> {
        let inspect = self.docker.inspect_container(&self.id, None).await?;
        debug!(?inspect, "docker inspect");
        let ip = &inspect
            .network_settings
            .as_ref()
            .and_then(|s| s.networks.as_ref())
            .and_then(|ns| ns.get("bridge"))
            .and_then(|n| n.ip_address.as_ref())
            .ok_or_else(|| Error::Inspection {
                message: "couldn't extract container IP".to_owned(),
            })?;

        let ip_addr = ip
            .parse()
            .unwrap_or_else(|_| panic!("failed to parse ip: {}", ip));
        Ok(ip_addr)
    }

    pub async fn get_label(&self, label: &str) -> Result<Option<String>, Error> {
        let inspect = self.docker.inspect_container(&self.id, None).await?;
        let result = inspect
            .config
            .and_then(|c| c.labels)
            .and_then(|mut ls| ls.remove(label));

        Ok(result)
    }

    pub async fn wait_for_port_listening(&self, port: u16, timeout: Duration) -> Result<(), Error> {
        let ip = self.ip().await?;
        let started_at = Instant::now();
        loop {
            if TcpStream::connect(SocketAddr::new(ip, port)).await.is_err() {
                if started_at.elapsed() >= timeout {
                    return Err(Error::WaitForPortTimeout);
                }
                sleep(Duration::from_millis(100)).await;
            } else {
                return Ok(());
            }
        }
    }

    /// Execute inside of container
    pub async fn exec(&self, cmd: Vec<&str>) -> Result<(), Error> {
        let create_res = self
            .docker
            .create_exec(
                &self.id,
                CreateExecOptions {
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(true),
                    cmd: Some(cmd),
                    ..Default::default()
                },
            )
            .await?;
        let mut output = match self.docker.start_exec(&create_res.id, None).await? {
            StartExecResults::Attached { output, .. } => output,
            _ => panic!("exec should be attached"),
        };
        while let Some(log_output_res) = output.next().await {
            match log_output_res? {
                LogOutput::StdOut { message } => {
                    let msg = std::str::from_utf8(&message).expect("utf8 str");
                    info!(msg, "exec");
                }
                LogOutput::StdErr { message } => {
                    let msg = std::str::from_utf8(&message).expect("utf8 str");
                    warn!(msg, "exec");
                }
                other => panic!("unsupported exec log output: {:?}", other),
            }
        }

        Ok(())
    }
}

/// Dropping the flag starts counting down (with timeout 10s) to the container
/// purge
#[derive(Debug)]
struct AliveFlag(TcpStream);
