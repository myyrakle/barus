fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // 클라이언트 코드도 생성
        .compile_protos(&["proto/barus.proto"], &["proto"])?;
    Ok(())
}
