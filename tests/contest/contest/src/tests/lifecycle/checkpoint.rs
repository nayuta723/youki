use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::anyhow;
use test_framework::TestResult;

use super::get_result_from_output;
use crate::utils::get_runtime_path;
use crate::utils::test_utils::State;

// Simple function to figure out the PID of the first container process
fn get_container_pid(project_path: &Path, id: &str) -> Result<i32, TestResult> {
    let res_state = match Command::new(get_runtime_path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--root")
        .arg(project_path.join("runtime"))
        .arg("state")
        .arg(id)
        .spawn()
        .expect("failed to execute state command")
        .wait_with_output()
    {
        Ok(o) => o,
        Err(e) => {
            return Err(TestResult::Failed(anyhow!(
                "error getting container state {}",
                e
            )));
        }
    };
    let stdout = match String::from_utf8(res_state.stdout) {
        Ok(s) => s,
        Err(e) => {
            return Err(TestResult::Failed(anyhow!(
                "failed to parse container stdout {}",
                e
            )));
        }
    };
    let state: State = match serde_json::from_str(&stdout) {
        Ok(v) => v,
        Err(e) => {
            return Err(TestResult::Failed(anyhow!(
                "error in parsing state of container: stdout : {}, parse error : {}",
                stdout,
                e
            )));
        }
    };

    Ok(state.pid.unwrap_or(-1))
}

// CRIU requires a minimal network setup in the network namespace
fn setup_network_namespace(project_path: &Path, id: &str) -> Result<(), TestResult> {
    let pid = get_container_pid(project_path, id)?;

    if let Err(e) = Command::new("nsenter")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("-t")
        .arg(format!("{pid}"))
        .arg("-a")
        .args(vec!["/bin/ip", "link", "set", "up", "dev", "lo"])
        .spawn()
        .expect("failed to exec ip")
        .wait_with_output()
    {
        return Err(TestResult::Failed(anyhow!(
            "error setting up network namespace {}",
            e
        )));
    }

    Ok(())
}

fn checkpoint(
    project_path: &Path,
    id: &str,
    args: Vec<&str>,
    work_path: Option<&str>,
    image_dir: Option<&Path>,
) -> TestResult {
    if let Err(e) = setup_network_namespace(project_path, id) {
        return e;
    }

    // If no image_dir is provided, create a temporary directory.
    let temp_dir;
    let checkpoint_dir = match image_dir {
        Some(dir) => dir.to_path_buf(),
        None => {
            temp_dir = match tempfile::tempdir() {
                Ok(td) => td,
                Err(e) => {
                    return TestResult::Failed(anyhow::anyhow!(
                        "failed creating temporary directory {:?}",
                        e
                    ));
                }
            };
            let dir = temp_dir.as_ref().join("checkpoint");
            if let Err(e) = std::fs::create_dir(&dir) {
                return TestResult::Failed(anyhow::anyhow!(
                    "failed creating checkpoint directory ({:?}): {}",
                    &dir,
                    e
                ));
            }
            dir
        }
    };

    let additional_args = match work_path {
        Some(wp) => vec!["--work-path", wp],
        _ => Vec::new(),
    };

    let runtime_path = get_runtime_path();

    let checkpoint = Command::new(runtime_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--root")
        .arg(project_path.join("runtime"))
        .arg(match runtime_path {
            _ if runtime_path.ends_with("youki") => "checkpointt",
            _ => "checkpoint",
        })
        .arg("--image-path")
        .arg(&checkpoint_dir)
        .args(additional_args)
        .args(args)
        .arg(id)
        .spawn()
        .expect("failed to execute checkpoint command")
        .wait_with_output();

    if let Err(e) = get_result_from_output(checkpoint) {
        return TestResult::Failed(anyhow::anyhow!("failed to execute checkpoint command: {e}"));
    }

    // Check for complete checkpoint
    if !Path::new(&checkpoint_dir.join("inventory.img")).exists() {
        return TestResult::Failed(anyhow::anyhow!(
            "resulting checkpoint does not seem to be complete. {:?}/inventory.img is missing",
            &checkpoint_dir,
        ));
    }

    if !Path::new(&checkpoint_dir.join("descriptors.json")).exists() {
        return TestResult::Failed(anyhow::anyhow!(
            "resulting checkpoint does not seem to be complete. {:?}/descriptors.json is missing",
            &checkpoint_dir,
        ));
    }

    let dump_log = match work_path {
        Some(wp) => Path::new(wp).join("dump.log"),
        _ => checkpoint_dir.join("dump.log"),
    };

    if !dump_log.exists() {
        return TestResult::Failed(anyhow::anyhow!(
            "resulting checkpoint log file {:?} not found.",
            &dump_log,
        ));
    }

    TestResult::Passed
}

/// Check that the network namespace was treated as external by CRIU.
/// Fails if netns-*.img is absent or lacks ext_key="extRootNetNS".
/// CRIU img files embed protobuf strings as raw UTF-8, so a byte search suffices.
pub fn check_external_netns(checkpoint_dir: &Path) -> Result<(), TestResult> {
    let netns_img = std::fs::read_dir(checkpoint_dir)
        .map_err(|e| TestResult::Failed(anyhow::anyhow!("failed to read dir: {}", e)))?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("netns-"))
        .map(|e| e.path());

    let img = netns_img.ok_or_else(|| {
        TestResult::Failed(anyhow::anyhow!(
            "netns-*.img not found in {:?}: network namespace image is missing",
            checkpoint_dir,
        ))
    })?;

    let bytes = std::fs::read(&img)
        .map_err(|e| TestResult::Failed(anyhow::anyhow!("failed to read {:?}: {}", img, e)))?;
    if !bytes.windows(12).any(|w| w == b"extRootNetNS") {
        return Err(TestResult::Failed(anyhow::anyhow!(
            "{:?} does not contain ext_key=extRootNetNS: network namespace was not treated as external",
            img,
        )));
    }

    Ok(())
}

/// Check that the PID namespace was treated as external by CRIU.
/// Fails if pidns-*.img is absent or lacks ext_key="extRootPidNS".
pub fn check_external_pidns(checkpoint_dir: &Path) -> Result<(), TestResult> {
    let pidns_img = std::fs::read_dir(checkpoint_dir)
        .map_err(|e| TestResult::Failed(anyhow::anyhow!("failed to read dir: {}", e)))?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("pidns-"))
        .map(|e| e.path());

    let img = pidns_img.ok_or_else(|| {
        TestResult::Failed(anyhow::anyhow!(
            "pidns-*.img not found in {:?}: PID namespace image is missing",
            checkpoint_dir,
        ))
    })?;

    let bytes = std::fs::read(&img)
        .map_err(|e| TestResult::Failed(anyhow::anyhow!("failed to read {:?}: {}", img, e)))?;
    if !bytes.windows(12).any(|w| w == b"extRootPidNS") {
        return Err(TestResult::Failed(anyhow::anyhow!(
            "{:?} does not contain ext_key=extRootPidNS: PID namespace was not treated as external",
            img,
        )));
    }

    Ok(())
}

pub fn checkpoint_leave_running_work_path_tmp(project_path: &Path, id: &str) -> TestResult {
    checkpoint(project_path, id, vec!["--leave-running"], Some("/tmp/"), None)
}

pub fn checkpoint_leave_running(project_path: &Path, id: &str) -> TestResult {
    checkpoint(project_path, id, vec!["--leave-running"], None, None)
}

/// Checkpoint a container started with external network and PID namespaces.
/// Verifies that CRIU recorded both namespaces as external.
pub fn checkpoint_with_external_namespaces(project_path: &Path, id: &str) -> TestResult {
    let temp_dir = match tempfile::tempdir() {
        Ok(td) => td,
        Err(e) => {
            return TestResult::Failed(anyhow::anyhow!(
                "failed creating temporary directory {:?}",
                e
            ));
        }
    };
    let image_dir = temp_dir.as_ref().join("checkpoint");
    if let Err(e) = std::fs::create_dir(&image_dir) {
        return TestResult::Failed(anyhow::anyhow!(
            "failed creating checkpoint directory ({:?}): {}",
            &image_dir,
            e
        ));
    }

    let result = checkpoint(
        project_path,
        id,
        vec!["--leave-running"],
        None,
        Some(&image_dir),
    );
    if !matches!(result, TestResult::Passed) {
        return result;
    }

    if let Err(e) = check_external_netns(&image_dir) {
        return e;
    }

    if let Err(e) = check_external_pidns(&image_dir) {
        return e;
    }

    TestResult::Passed
}
