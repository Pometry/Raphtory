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
    * Remove all " and '. (This can be done in a text editor)
    * Add `\n` wherever there is a `sent_mail` : 
    `sed 's!sent_mail!\
quote> !g' < output.csv`
and

    * Replace "Message-ID" with line break: `sed 's/Message-ID/\n/g' output.csv > output1.csv` and `sed 's/Message-Id/\n/g' output1.csv > output2.csv` and `sed 's/Message-id/\n/g' output2.csv > output3.csv` 

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.enron.Runner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-enron](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-enron). If you have downloaded the Examples folder from the installation guide previously, then the Enron example will already be set up. If not, please return [there](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html) and complete this step first.
2. Download the _email_test.csv_ data from [raphtory-data](https://github.com/Raphtory/Data) repository and place it inside of the resources folder.
3. In the Examples folder, open up the directory `raphtory-example-enron` to get this example running.
4. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](https://raphtory.readthedocs.io/en/development/PythonClient/tutorial.html). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `src/main/python/EnronJupyterNotebook.ipynb`.
5. You are now ready to run this example. You can either run this example via Intellij by running the class `Runner.scala` or [via sbt](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html#running-raphtory-via-sbt).
6. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.
