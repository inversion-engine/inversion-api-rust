use predicates::prelude::*;

#[test]
fn test_1() {
    let sub_cmd = assert_cmd::cargo::cargo_bin("test-1-inv-broker-sub-proc");
    let mut cmd = assert_cmd::Command::cargo_bin("test-1-inv-broker-entry-proc")
        .unwrap();
    cmd.arg(sub_cmd);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("#test-1-inv-broker-entry-proc started"))
        .stdout(predicate::str::contains("#test-1-inv-broker-sub-proc started"))
        ;
}
