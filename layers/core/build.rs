fn main() {
    // Re-run this build script (and recompile the crate) whenever
    // NAUKA_VERSION changes. Without this, cargo may serve a cached
    // build that has a stale option_env!("NAUKA_VERSION") value.
    println!("cargo::rerun-if-env-changed=NAUKA_VERSION");
}
