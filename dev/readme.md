# Quick start dev setup

If you run `./dev/bootstrap.sh` it will setup the environment for you.

In this case it will 

- Install a rust nightly
- setup your git hooks to point to the dev hooks in `./dev/hooks`

You need to run this only once, you will have to `chmod +x ./dev/bootstrap.sh` first.

## dev hooks

### pre-commit hook

The pre-commit hook will run `cargo fmt` on all staged files. If the formatting fails, the commit will be aborted.


    #!/bin/bash
    
    # Run rustfmt on the whole repository using nightly toolchain
    cargo +nightly fmt --all -- --check
    
    # Capture the exit code of the previous command
    RESULT=$?
    
    # If the result is non-zero (i.e., there were formatting errors), abort the commit
    if [ $RESULT -ne 0 ]; then
    echo "There are formatting errors. Please run 'cargo +nightly fmt' and fix them before committing."
    exit 1
    fi


