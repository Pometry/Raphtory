# Test

- Ensure test dependencies are installed
    ```bash
    $ python -m pip install -q pytest networkx numpy seaborn pandas nbmake pytest-xdist matplotlib
    ```

- To run `raphtory` python tests
    ```bash
    $ cd python && pytest
    ```

- To run notebook tests
    ```bash
    $ cd python/tests && pytest --nbmake --nbmake-timeout=1200 .
    ```
