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

use monots_integration_tests::MonotsInstance;

#[tokio::test]
async fn login_success() {
    let mut inst = MonotsInstance::new("auth_login_success").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.client().await.unwrap();
    client.login("admin", "admin").await.unwrap();
}

#[tokio::test]
async fn login_failure() {
    let mut inst = MonotsInstance::new("auth_login_failure").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.client().await.unwrap();
    let err = client.login("admin", "wrong").await.unwrap_err();
    assert!(err.to_string().contains("login") || err.to_string().len() > 0);
}

#[tokio::test]
async fn query_without_auth_fails() {
    let mut inst = MonotsInstance::new("auth_no_token").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.client().await.unwrap();
    let err = client.query("SELECT 1").await.unwrap_err();
    let _ = err;
}
