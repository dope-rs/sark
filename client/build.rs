fn main() {
    println!("cargo:rerun-if-env-changed=SARK_ALLOW_X86");
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let allow = std::env::var("SARK_ALLOW_X86").is_ok();
    if arch != "aarch64" && arch != "arm64" && !allow {
        panic!(
            "void workspace targets ARM64/AArch64 (set SARK_ALLOW_X86=1 to allow local dev on {});",
            arch
        );
    }
}
