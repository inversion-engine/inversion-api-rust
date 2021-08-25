#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("#test-1-inv-broker-entry-proc started");
    let sub_proc = std::env::args()
        .skip(1)
        .next()
        .expect("required subproc path as process argument");
    std::process::Command::new(&sub_proc)
        .status()
        .expect(&format!("running sub-process failed: {}", sub_proc));
}
