// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use config::config::NodeConfig;
use failure::prelude::*;
use logger::prelude::*;
use regex::Regex;
use std::{fs, net::IpAddr, path::PathBuf, str::FromStr};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "executable")]
pub enum Executable {
    /// Submit TXNs to test if Libra is alive.
    #[structopt(name = "testliveness")]
    TestLiveness,
    /// Submit TXNs and measure client throughput.
    #[structopt(name = "measurethroughput")]
    MeasureThroughput,
    /// Linear search max throughput with stairwise submission rate.
    #[structopt(name = "searchmaxthroughput")]
    SearchMaxThroughput(SearchMaxThroughput),
}

#[derive(Debug, StructOpt)]
pub struct SearchMaxThroughput {
    /// Upper bound value of submission rate for each client.
    #[structopt(short = "u", long = "upper_bound", default_value = "100")]
    pub upper_bound: u64,
    /// Upper bound value of submission rate for each client.
    #[structopt(short = "l", long = "lower_bound", default_value = "10")]
    pub lower_bound: u64,
    /// Increase step of submission rate for each client.
    #[structopt(short = "i", long = "inc_step", default_value = "10")]
    pub inc_step: u64,
    /// How many times to repeat the same linear search. Each time with new accounts/TXNs.
    #[structopt(short = "s", long = "num_searches", default_value = "10")]
    pub num_searches: u64,
}

/// CLI options for RuBen.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "RuBen",
    author = "Libra",
    about = "RuBen (Ru)ns The Libra (Ben)chmarker For You."
)]
pub struct Opt {
    /// Validator address list seperated by whitespace: `ip_address:port ip_address:port ...`.
    /// It is requried unless (and hence conflict with) swarm_config_dir is present.
    #[structopt(
        short = "a",
        long = "validator_addresses",
        conflicts_with = "swarm_config_dir",
        requires = "debug_address",
        required_unless = "swarm_config_dir"
    )]
    pub validator_addresses: Vec<String>,
    /// TODO: Discard this option. Debug interface address in the form of ip_address:port.
    /// It is requried unless (and hence conflict with) swarm_config_dir is present.
    #[structopt(
        short = "d",
        long = "debug_address",
        conflicts_with = "swarm_config_dir",
        requires = "validator_addresses",
        required_unless = "swarm_config_dir"
    )]
    pub debug_address: Option<String>,
    /// libra_swarm's config file directory, which holds libra_node's config .toml file(s).
    /// It is requried unless (and hence conflict with)
    /// validator_addresses and debug_address are both present.
    #[structopt(
        short = "s",
        long = "swarm_config_dir",
        raw(conflicts_with_all = r#"&["validator_addresses", "debug_address"]"#),
        raw(required_unless_all = r#"&["validator_addresses", "debug_address"]"#)
    )]
    pub swarm_config_dir: Option<String>,
    /// Metrics server process's address.
    /// If this argument is not present, RuBen will not spawn metrics server.
    #[structopt(short = "m", long = "metrics_server_address")]
    pub metrics_server_address: Option<String>,
    /// Valid faucet key file path.
    #[structopt(short = "f", long = "faucet_key_file_path", required = true)]
    pub faucet_key_file_path: String,
    /// Number of accounts to create in Libra.
    #[structopt(short = "n", long = "num_accounts", default_value = "32")]
    pub num_accounts: u64,
    /// Free lunch amount to accounts.
    #[structopt(short = "l", long = "free_lunch", default_value = "1000000")]
    pub free_lunch: u64,
    /// Number of AC clients.
    /// If not specified or equals 0, it will be set to validator_addresses.len().
    #[structopt(short = "c", long = "num_clients", default_value = "0")]
    pub num_clients: usize,
    /// Randomly distribute the clients to start sending requests over the stagger_range_ms time.
    /// A value of 1 ms effectively means starting all clients at once.
    #[structopt(short = "g", long = "stagger_range_ms", default_value = "64")]
    pub stagger_range_ms: u16,
    /// Number of repetition to attempt, in one epoch, to increase overal number of sent TXNs.
    #[structopt(short = "r", long = "num_rounds", default_value = "1")]
    pub num_rounds: u64,
    /// Number of epochs to measure the TXN throughput, each time with newly created Benchmarker.
    #[structopt(short = "e", long = "num_epochs", default_value = "10")]
    pub num_epochs: u64,
    /// Submit constant number of TXN requests per second; otherwise TXNs are flood to Libra.
    #[structopt(short = "k", long = "const_rate")]
    pub const_rate: Option<u64>,
    /// Supported applications of Benchmark library.
    #[structopt(subcommand)]
    pub executable: Executable,
}

/// Helper that checks if address is valid, and converts unspecified address to localhost.
/// If parsing as numeric network address fails, treat as valid domain name.
fn parse_socket_address(address: &str, port: u16) -> String {
    match IpAddr::from_str(address) {
        Ok(ip_address) => {
            if ip_address.is_unspecified() {
                format!("localhost:{}", port)
            } else {
                format!("{}:{}", address, port)
            }
        }
        Err(_) => format!("{}:{}", address, port),
    }
}

/// Scan *.node.config.toml files under config_dir_name, parse them as node config
/// and return libra_swarm's node addresses info as a vector.
fn parse_swarm_config_from_dir(config_dir_name: &str) -> Result<Vec<String>> {
    let mut validator_addresses: Vec<String> = Vec::new();
    let re = Regex::new(r"[[:alnum:]]{64}\.node\.config\.toml").expect("failed to build regex");
    let config_dir = PathBuf::from(config_dir_name);
    if config_dir.is_dir() {
        for entry in fs::read_dir(config_dir).expect("invalid config directory") {
            let path = entry.expect("invalid path under config directory").path();
            if path.is_file() {
                let filename = path
                    .file_name()
                    .expect("failed to convert filename to string")
                    .to_str()
                    .expect("failed to convert filename to string");
                if re.is_match(filename) {
                    debug!("Parsing node config file {:?}.", filename);
                    let config_string = fs::read_to_string(&path)
                        .unwrap_or_else(|_| panic!("failed to load config file {:?}", filename));
                    let config = NodeConfig::parse(&config_string).unwrap_or_else(|_| {
                        panic!("failed to parse NodeConfig from {:?}", filename)
                    });
                    let address = parse_socket_address(
                        &config.admission_control.address,
                        config.admission_control.admission_control_service_port,
                    );
                    validator_addresses.push(address);
                }
            }
        }
    }
    if validator_addresses.is_empty() {
        bail!(
            "unable to parse validator_addresses from {}",
            config_dir_name
        )
    }
    Ok(validator_addresses)
}

impl Opt {
    pub fn new_from_args() -> Self {
        let mut args = Opt::from_args();
        args.try_parse_validator_addresses();
        if args.num_clients == 0 {
            args.num_clients = args.validator_addresses.len();
        }
        args.check_linear_search_args();
        args
    }

    /// Override validator_addresses and debug_address if swarm_config_dir is provided.
    pub fn try_parse_validator_addresses(&mut self) {
        if let Some(swarm_config_dir) = &self.swarm_config_dir {
            let validator_addresses =
                parse_swarm_config_from_dir(swarm_config_dir).expect("invalid arguments");
            self.validator_addresses = validator_addresses;
        }
    }

    pub fn parse_submit_pattern(&self) -> u64 {
        self.const_rate.unwrap_or(std::u64::MAX)
    }

    fn check_linear_search_args(&self) {
        if let Executable::SearchMaxThroughput(sub_args) = &self.executable {
            assert!(sub_args.lower_bound > 0);
            assert!(sub_args.inc_step > 0);
            assert!(sub_args.lower_bound < sub_args.upper_bound);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ruben_opt::{parse_socket_address, parse_swarm_config_from_dir};

    #[test]
    fn test_parse_socket_address() {
        assert_eq!(
            parse_socket_address("216.10.234.56", 12345),
            "216.10.234.56:12345"
        );
        assert_eq!(parse_socket_address("0.0.0.0", 12345), "localhost:12345");
        assert_eq!(parse_socket_address("::", 12345), "localhost:12345");
        assert_eq!(
            parse_socket_address("face:booc::0", 12345),
            "face:booc::0:12345"
        );
        assert_eq!(
            parse_socket_address("2401:dbff:121f:a2f1:face:d:6c:0", 12345),
            "2401:dbff:121f:a2f1:face:d:6c:0:12345"
        );
        assert_eq!(parse_socket_address("localhost", 12345), "localhost:12345");
        assert_eq!(
            parse_socket_address("www.facebook.com", 12345),
            "www.facebook.com:12345"
        );
    }

    #[test]
    fn test_parse_swarm_config_from_invalid_dir() {
        // Directory doesn't exist at all.
        let non_exist_dir_name = String::from("NonExistDir");
        let mut result = parse_swarm_config_from_dir(&non_exist_dir_name);
        assert_eq!(result.is_err(), true);

        // Directory exists but config file does not.
        let path = std::env::current_dir().expect("unable to get current dir");
        if let Some(dir_without_config) = path.to_str() {
            result = parse_swarm_config_from_dir(&dir_without_config);
            assert_eq!(result.is_err(), true);
        }
    }
}
