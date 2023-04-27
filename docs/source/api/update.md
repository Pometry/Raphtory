# Build

Raphtory documentations can be found in `docs` directory. 
They are built using [Sphinx](https://www.sphinx-doc.org/en/master/) and hosted by readthedocs. 

After making your changes, you're good to build them. 

- Ensure that all development dependencies are already installed.
    ```bash
    $ cd docs && pip install -q -r requirements.txt
    ```

- Build rust specific documentation
    ```bash
    $ make rust-build-docs
    ```

- Build docs
    ```bash
    $ cd docs && make html
    ```

- View docs
    ```bash
    $ open build/html/index.html
    ```
