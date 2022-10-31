## Steps to build docs
1. Create conda environment
    ```shell
    % conda create --name raphtorydocs python=3.9.13 -c conda-forge
    ```
2. Activate conda environment
   ```shell
   % conda activate raphtorydocs
   ```
3. Install dev dependencies
   ```shell
   % pip install myst-parser
   % pip install sphinx-rtd-theme
   % pip install sphinx==4.4.0
   % pip install docutils==0.17.1
   % pip install sphinx-tabs
   ```
4. Build docs
   ```shell
   % make html
   ```
