# Releasing Raphtory

There are three ways to release Raphtory

1. Automatic using Github Workflows
2. Manually using Github Workflows
3. Manually using the terminal

This document will explain each step.

## 1. Automatic using Github workflows

Go to the Github.com/Pometry/Raphtory page, and click on Actions

1. Click on the `(Manual Release) - Bumps all packages + PR` action. Run this workflow and select the PR and version increment. Given the version number is in `x.y.z` format
    - `patch` increments `z` by one
    - `minor` increments `y` by one
    - `major` increments `x` by one
2. This workflow will bump all the version numbers in the raphtory rust and python packages, and then will create a PR. You must then merge this PR into master and delete the branch.
3. Next run `(Automatic) Release Python, Rust & Github` action. This will pull from master, release a crate on rust, release packages to python and then will publish the source code on Github as a release


## 2. Manually using Github Workflows

The same process can be done above by instead using the manual workflows. These can be triggered in case the automatic release fails.

1. Click on the `(Manual Release) - Bumps all packages + PR` action. Run this workflow and select the PR and version increment. Given the version number is in `x.y.z` format
    - `patch` increments `z` by one
    - `minor` increments `y` by one
    - `major` increments `x` by one
2. Click on the `(Manual) Release Rust` action. Run this workflow. This will publish the crate to rust.
3. Click on the `(Manual) Release Python` action. Run this workflow. This will publish to pypi.
4. Click on the `(Manual) Release Github ` action. Run this workflow. This will publish to GitHub.


## 3. Manually using the terminal

These are the commands you can run in your terminal to manually release the code.

0. Decide whether you want a `patch`, `minor` or `major` release
1. **Bump the version** numbers of the packages locally by running `cargo release --execute --no-confirm --no-publish --no-push --no-tag patch|minor|major`
2. Commit this into a branch and push
3. Next **release the rust** package to crates by running `cargo publish --token CRATES_TOKEN --package raphtory --allow-dirty` Ensure you have a valid CRATES_TOKEN. You can generate this at `crates.io` but it must have permissions to publish to the `raphtory` crate otherwise it will fail
4. Release the packages to pypi for python. Python Raphtory uses maturin, you can build a local release for your own OS and publish this to PyPi by running below. note: You will be prompted for your pypi username and password for upload

   `cd python && maturin publish`

For all other distributions, you will have to launch a docker container for that architecture, install rust, conda, setup a python env, clone the repository, install maturin and then use the command above.

e.g. for manylinux2014_x86_x64. note: You will be prompted for your pypi username and password for upload

    docker run -it quay.io/pypa/manylinux2014_x86_64 bash
    curl https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly
    source "$HOME/.cargo/env"
    git clone https://github.com/Pometry/Raphtory.git
    cd Raphtory/python
    /opt/python/cp310-cp310/bin/pip install maturin
    /opt/python/cp310-cp310/bin/maturin publish -i "/opt/python/cp310-cp310/bin/python" 

5. Create a github release by going into [https://github.com/Pometry/Raphtory/releases/new](https://github.com/Pometry/Raphtory/releases/new). Setting the branch to the one you created above and creating the relevant tag.   
    



