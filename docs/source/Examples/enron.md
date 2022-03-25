# Suspicious Email Threads - ENRON Scandal

## Overview
[Enron Corporation](https://en.wikipedia.org/wiki/Enron) was an American energy, commodities, and services company based in Houston, Texas. In 2001, it was found that Enron used accounting loopholes and poor financial reporting to hide billions of dollars in debt from failed deals and projects. During its investigation, [Enron's email communication network](https://snap.stanford.edu/data/email-Enron.html) was made public and posted to the internet. Nodes in this dataset are email addresses and this dataset contains around 36692 nodes. Before analysis in Raphtory, the dataset needs cleaning. Instructions on how to clean the dataset are in the section `Cleaning the data` below. Once the data is cleaned, it is ready for ingestion and analysis by Raphtory. 

We have implemented the Connected Components algorithm which will organise email addresses into clusters of connected emails. This can be useful in identifying frequent emails going back and forth by those involved in the scandal. This dataset includes the text in the emails, so potential emails directly involved in the scandal can be identified.

## Project Overview

This [example](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-enron) builds a graph from the [Enron email dataset](https://www.kaggle.com/wcukierski/enron-email-dataset) and runs a query to tell us emails all connected by paths to each other.

In the examples folder you will find `EnronGraphBuilder.scala` and `Runner.scala`.

* `EnronGraphBuilder.scala` parses the data and builds the graph
* `Runner.scala` runs the application including the analysis

We have also included python scripts in directory`src/main/python` to output the dataframes straight into Jupyter notebook.

## Cleaning the data

The original enron dataset is pretty complex and requires processing so we have provided two options:

1) **Sample Data of Ten**: Test dataset that is already processed containing ten items can be downloaded from [here](https://github.com/Raphtory/Data/blob/main/email_test.csv), once downloaded, place the csv file into the `resources` folder. Each line contains an email with the message ID (unique email ID), date of email, sender and receiver.

2) **Full Dataset**: Download the original dataset from [Kaggle](https://www.kaggle.com/wcukierski/enron-email-dataset).
    * Remove all `\n` chars from file with:  `tr -d '\n' < input.csv > output.csv` 
    * Remove all " and ' : `sed 's/\"//g' output.csv > outputDoubleQuote.csv` and `sed "s/\'//g" outputDoubleQuote.csv > outputSingleQuote.csv`
    * Add `\n` wherever there is a `sent_mail` : 
    `sed 's!sent_mail!\
quote> !g' < outputSingleQuote.csv`
and

    * Replace "Message-ID" with line break: `sed 's/Message-ID/\n/g' outputSingleQuote.csv > output1.csv` and `sed 's/Message-Id/\n/g' output1.csv > output2.csv` and `sed 's/Message-id/\n/g' output2.csv > output3.csv` 

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.enron.Runner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-enron](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-enron). If you have downloaded the Examples folder from the installation guide previously, then the Enron example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-enron` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `src/main/python/EnronJupyterNotebook.ipynb`.
4. You are now ready to run this example. You can either run this example via Intellij by running the class `Runner.scala` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt)
5. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

The Enron email dataset is incredibly large and complex, therefore it takes a while for Raphtory to get through and analyse all the data.

Logs such as: 
```bash
15:48:27.727 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646321862675': Perspective '970557940000' finished in 114285 ms.
15:48:27.728 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646321862675: Running query, processed 8 perspectives.
```
```bash
15:52:18.400 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646321862675': Perspective '989557940000' finished in 6593 ms.
15:52:18.401 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646321862675: Running query, processed 27 perspectives.
15:52:24.698 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646321862675': Perspective '989858340000' finished in 6298 ms.
15:52:24.698 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646321862675: Running query, processed 28 perspectives.
15:52:24.792 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646321862675: Query completed with 28 perspectives and finished in 881069 ms.
```
are a good indication that the query is being run.