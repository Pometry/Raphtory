# Config file


<p style="margin-left: 1.5em;"> This is the configuration file (conf.py) which stores common variables. </p>


```python
PULSAR_ADDRESS = "pulsar://127.0.0.1:6650"
```
Address for the pulsar instance

```python
PULSAR_ADMIN_ADDRESS = "http://127.0.0.1:8080"
```
Address for the pulsar admin


```python
ZOOKEEPER_ADDRESS = "127.0.0.1:2181"
```
Address for the zookeeper instance


```python
SPOUT_TOPIC = "raphtory_data_raw"
```
Spout topic to read from


```python
TENANT = "public"
```
Tenant hosting the topics


```python
NAMESPACE = "default"
```
Namespace hosting the topics