<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Publish Docker build environment images

The GitHub Actions build workflows produce standard and no-AVX2 Docker images for `linux/amd64`. Each image is available as a workflow artifact for seven days.

## Download an image

1. Sign in to GitHub and open the repository's **Actions** tab.
2. Select the workflow for your Doris version. Open a recent run in which **Build Docker Images** succeeded.
3. Under **Artifacts** on the run summary, download the image you want and extract the ZIP file. It contains a `.tar` archive with the same name as the artifact.

| Workflow | Artifact | Default Docker Hub image |
| --- | --- | --- |
| Build | `docker-image-amd64` | `apache/doris:build-env-ldb-toolchain-latest` |
| Build | `docker-image-amd64-no-avx2` | `apache/doris:build-env-ldb-toolchain-no-avx2-latest` |
| Build (4.0) | `docker-image-4.0-amd64` | `apache/doris:build-env-ldb-toolchain-4.0-latest` |
| Build (4.0) | `docker-image-4.0-amd64-no-avx2` | `apache/doris:build-env-ldb-toolchain-4.0-no-avx2-latest` |
| Build (4.1) | `docker-image-4.1-amd64` | `apache/doris:build-env-ldb-toolchain-4.1-latest` |
| Build (4.1) | `docker-image-4.1-amd64-no-avx2` | `apache/doris:build-env-ldb-toolchain-4.1-no-avx2-latest` |
| Build (4.2) | `docker-image-4.2-amd64` | `apache/doris:build-env-ldb-toolchain-4.2-latest` |
| Build (4.2) | `docker-image-4.2-amd64-no-avx2` | `apache/doris:build-env-ldb-toolchain-4.2-no-avx2-latest` |

To download with the GitHub CLI, sign in with `gh auth login` if needed. Replace `RUN_ID` with the number after `/actions/runs/` in the run URL. This command downloads and extracts the standard 4.2 image archive into the current directory. [GitHub CLI documentation](https://cli.github.com/manual/gh_run_download)

```bash
gh run download RUN_ID --repo apache/doris-thirdparty --name docker-image-4.2-amd64
```

If you need a fresh artifact, select **Run workflow** on `main`. For **Build**, set `docker_only` to `true` to reuse the published Linux archive. The archive must match the latest change to `thirdparty/` in Doris. For the 4.x workflows, leave `force_build` at its default of `true` to build the dependencies before exporting the images.

## Load and push the image

Load the extracted archive into Docker. The command prints the image tags it restored. If the workflow used custom tags, use the tag printed by Docker for the image you want to publish. [Docker image load reference](https://docs.docker.com/reference/cli/docker/image/load/)

```bash
docker image load --input docker-image-4.2-amd64.tar
```

Sign in to Docker Hub with an account that can push to the destination repository. The following commands push the standard 4.2 image to `apache/doris` with its default tag. For a different version or variant, use the corresponding tag above. [Docker Hub push guide](https://docs.docker.com/docker-hub/repos/manage/hub-images/push/)

```bash
docker login
docker image push apache/doris:build-env-ldb-toolchain-4.2-latest
```

To publish to your own Docker Hub repository, tag the loaded image with the destination name before pushing. Replace `YOUR_NAMESPACE` with your Docker Hub username or organization. [Docker image tag reference](https://docs.docker.com/reference/cli/docker/image/tag/)

```bash
docker image tag apache/doris:build-env-ldb-toolchain-4.2-latest YOUR_NAMESPACE/doris:build-env-ldb-toolchain-4.2-latest
docker image push YOUR_NAMESPACE/doris:build-env-ldb-toolchain-4.2-latest
```
