## Steps to build docs
1. Start in the root raphtory folder
2. Install requirements for readthedocs 
3. Build raphtory and pyraphtory 
    ```shell
    make sbt-build
    make python-build
    ```
4. Install dev dependencies
   ```shell
   pip install myst-parser sphinx-rtd-theme sphinx docutils sphinx-tabs nbsphinx
   ```
5. Build docs
   ```shell
   cd docs && make html
   ```
6. View docs
   ```shell
   open build/html/index.html
   ```
   
If you are not editing the scaladocs, these can be disabled which will greatly speed up build times. 
Just edit the `docs/source/conf.py` and comment out the `'extractScalaAlgoDocs',` line with a `#`
