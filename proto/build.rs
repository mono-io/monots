// Copyright 2026 MonoTS Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/tsdb.proto");
    println!("cargo:rerun-if-changed=proto/metadata.proto");
    println!("cargo:rerun-if-changed=proto/stream.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/tsdb.proto",
                "proto/metadata.proto",
                "proto/stream.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
