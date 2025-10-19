#[derive(Debug, Clone)]
pub struct SystemInfo {
    pub total_memory: u64, // bytes
    pub cpu_count: usize,  // number of CPU cores (hyperthreaded cores included)
}

pub fn get_system_info() -> SystemInfo {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    let total_memory = sys.total_memory();
    let cpu_count = sys.cpus().len();

    SystemInfo {
        total_memory,
        cpu_count,
    }
}
