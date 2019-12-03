﻿![](images/hn_logo.png)﻿  
Hacker News, (https://news.ycombinator.com/), is a social news website focusing on computer science and entrepreneurship. It is run by Paul Graham's investment fund and startup incubator, Y Combinator. In general, content that can be submitted is defined as "anything that gratifies one's intellectual curiosity (“Hacker News,” 2019). 
This project investigates the effect of media headlines sentiment on user engagement, reaction and participation. NLP (Natural Language Processing) techniques are used to prepare a corpus, as harvested from the posted headlines on HN, and from there apply further sentiment and statistical analysis. 

This projects seeks to find a correltation between headline sentiment and user comments sentiment. By applying NLP techniques and framing the solution in a Spark ingestion pipeline, the uathor seek to present a solution which could eventually display real-time sentiment analysis as measured over several dimensions from posts made to the Hacke News forum.

Instructions:

Before attempting to re-produce the investigation’s result, make sure the following software programs are installed and working on your chosen platform – Windows, Mac or Linux
1)	Python 3 - https://www.python.org/downloads/
2)	Spark 3 for Apache Hadoop 2.7 - http://spark.apache.org/downloads.html
3)	NLTK 3.4.5 - https://www.nltk.org/install.html
4)	Jupyter Notebook - https://jupyter.org/install
5)	Google Cloud Account – Create a free trial Google Cloud Account (1 year). You’ll need this to access public data in Google BigQuery, as well as import the HN posts into a cutom created BigQuery table.

-	The above listed repository can be cloned or downloaded to disk. 
-	The Jupyter notebook, located in the code directory, will works out-of-the-box granted you have correctly installed Spark, Java, Jupyter and the NTLK.
-	Make sure all the relevant path variables are set for Hadoop, Spark and Java in order for the runtime executables to work. For correct path variable setup, refer to the installation instructions for Java and Spark.
