use std::fs;
use std::path::Path;

#[cfg(target_os = "windows")]
fn compile_windows_resources() {
    println!("cargo:rerun-if-changed=assets/icons/sms-gateway.ico");
    println!("cargo:rerun-if-changed=assets/icons/sms-gateway.rc");
    embed_resource::compile("assets/icons/sms-gateway.rc", embed_resource::NONE);
}

fn emit_rerun_for_dir(path: &Path) {
    if !path.exists() {
        return;
    }

    if path.is_file() {
        println!("cargo:rerun-if-changed={}", path.display());
        return;
    }

    let Ok(entries) = fs::read_dir(path) else {
        return;
    };

    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            emit_rerun_for_dir(&p);
        } else if p.is_file() {
            println!("cargo:rerun-if-changed={}", p.display());
        }
    }
}

fn main() {
    #[cfg(target_os = "windows")]
    compile_windows_resources();

    // frontend/dist is embedded into the Rust binary via rust-embed.
    // Re-run build when any embedded asset changes.
    println!("cargo:rerun-if-changed=frontend/dist");
    emit_rerun_for_dir(Path::new("frontend/dist"));
}
