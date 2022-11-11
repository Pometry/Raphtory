## Steps to build docs
1. Start in the root raphtory folder 
2. Build raphtory and pyraphtory 
    ```shell
    make sbt-build
    make python-build
    ```
3. Install dev dependencies
   ```shell
   pip install myst-parser sphinx-rtd-theme sphinx docutils sphinx-tabs
   ```
4. Build docs
   ```shell
   cd docs && make html
   ```
5. View docs
   ```shell
   open build/html/index.html
   ```
   
If you are not editing the scaladocs, these can be disabled which will greatly speed up build times. 
Just edit the `docs/source/conf.py` and comment out the `'extractScalaAlgoDocs',` line with a `#`
