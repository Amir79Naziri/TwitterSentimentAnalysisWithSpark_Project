<div align="center">
  
  <h1 align="center">Twitter Sentiment Analysis Using Spark</h1>
</div>

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#usage">Usage</a>
      <ul>
        <li><a href="#requirements">Requirements</a></li>
        <li><a href="#run">Run</a></li>
      </ul>
    <li>
      <a href="#phases">Phases</a>
      <ul>
        <li><a href="#Phase1">Introduce Data</a></li>
        <li><a href="#Phase2">Preprocess Data</a></li>
        <li><a href="#Phase3">Simulate Streaming</a></li>
        <li><a href="#Phase4">Introduce and Train model</a></li>
        <li><a href="#Phase5">Using model in Real-Time Senario</a></li>
        <li><a href="#Phase6">Further works!</a></li>
      </ul>
    </li>
    <li><a href="#contact">Contact</a></li> 
  </ol>
</details>

<br/>

## About The Project
This project implements a `sentiment analyzer` with `Spark ML` library. It uses tweets and retweets of Twitter Dataset for training and testing model. There are many phases in this project which complete the project implementation step by step. Steps are as follows:

* [Introduce Data](#Phase1)
* [Preprocess Data](#Phase2)
* [Simulate Streaming](#Phase3)
* [Introduce and Train model](#Phase4)
* [Using model in Real-Time Senario](#Phase5)
* [Further works!](#Phase6)


<p align="right">(<a href="#top">back to top</a>)</p>

## Usage  
### Requirements
Project uses `Spark(SparkSQL)` for data exploration, model training and Streaming. Therefore, Spark should be installed in order to run Notebooks. Moreover, It should be noted that this project is totally implemented in `python`, so in order to make `Spark` able to run codes, `PySpark` should be installed.
```bash
$ pip install pyspark
```
Also, project needs `SparkSQL` to use built-in `Dataframe` objects for querying data and training model. It should be noted that `RDD` is not used during this project.
```bash
$ pip install pyspark[sql]
```
__(optional)__   And finally, in order to set environmental variables for enabling Jupyter to run `Spark` code, `findspark` should be installed. (Also, setting environmental variables could be done manually without `findspark`)
```bash
$ pip install findspark
```

### Run 
Run all cells in order. Before running <b>stream_sentiment_analysis.ipynb</b> file, please run <b>simulate streaming.py</b> script.

<p align="right">(<a href="#top">back to top</a>)</p>

## Phases  
<br/>
<h3 id="Phase1">Introduce Data</h3>  

The data is gathered by <a href="http://help.sentiment140.com/for-students">Sentiment140</a> from Twitter API as a CSV (1,600,000 lines) with emotions removed. Data file format has 6 fields:
<ul>
    <li>The polarity of the tweet (0 = negative, 4 = positive)</li>
    <li>The id of the tweet (2087)</li>
    <li>The date of the tweet (Sat May 16 23:58:44 UTC 2009)</li>
    <li>The query (lyx). If there is no query, then this value is NO_QUERY.</li>
    <li>The user that tweeted (robotickilldozr)</li>
    <li>The text of the tweet (Lyx is cool) </li>
</ul>

For more details and downloading data, please visit <a href="http://help.sentiment140.com/for-students">here</a>.  

<br/>
<h3 id="Phase2">Preprocess Data</h3>  
For this phase <b>data_exploration.ipynb</b> file is implemented and explained in details. Also, around 300,000 lines of raw dataset have been randomly sampled and used for next phase (streaming simulation). 

<br/>
<h3 id="Phase3">Simulate Streaming</h3>  
In this phase, <b>simulate_streaming.py</b> script is implemented to simulate real-time data generation every one second. In each second, a new CSV file will be created as a new fetched tweet. Obviously, this time is very unrealistic; Hence, feel free to decrease it as much as possible to make it close to real.

<br/>
<h3 id="Phase4">Introduce and Train model</h3>  
In this phase <a href="https://en.wikipedia.org/wiki/Logistic_regression">Logistic Regression</a> is used in order to classify tweets. Also, Spark ML library is used for all machine learning algorithms. For more details about implementation, check <b>classifier_model.ipynb</b> file.

<br/>
<h3 id="Phase5">Using model in Real-Time Senario</h3>  
In this phase, <b>stream_sentiment_analysis.ipynb</b> file is implemented to simulate real-time data classification every five seconds. Obviously, this time is very unrealistic; Hence, feel free to decrease it as much as possible to make it close to real.

#### :warning: warning: Before running <b>stream_sentiment_analysis.ipynb</b> file, please run <b>simulate streaming.py</b> script.

<br/>
<h3 id="Phase6">Further works!</h3>  
This is a small project just to work with Spark. In real world, data must be in range of GB to use Spark. Also, using Logistic Regression might not be a good choice. Feel free to use other models such as Naive Bayes, SVM, and even Neural Networks. 
  
Also, all configurations are set to run codes on local, so in order to use codes on clusters, one should change configurations and even partitioning strategy!!


<p align="right">(<a href="#top">back to top</a>)</p>


## Contact
[Amirreza Naziri](https://github.com/Amir79Naziri)  
Email: naziriamirreza@gmail.com  

<p align="right">(<a href="#top">back to top</a>)</p>
