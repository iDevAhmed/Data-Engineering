# DEProject
Data Engineering Project Winter 2020

Project overview: The aim of this is project is to go over the data engineering process, find research questions and answers to them, and investigate further questions. The data has been collected from 3 datasets, The World Happiness Report, All 250 countries data and Life Expectancy data. The main aim is to find co-relation between happiness and life expectancy/diseases in 2015.

## Milestone 1
In Milestone 1, we are using the three datasets that contain information about many aspects of different countries including economical, educational, health and lifestyle, and the measured happiness of the people living in those countries. We tried to analyze and find correlations between the different aspects between those countries, like the happiness of the citizens and the disease, economy, employment rate, and some engineered features like population density.


## Milestone 2
In milestone 2, we use Airflow to schedule the extract, transform, load operations. We extract the data from the three datasets in intervals of time, clean the data and integrate the three datasets into one dataset. Finally, we use the resulting datasets to find correlations between different features like milestone 1.

## Milestone 3
In Milestone 3, we are using three datasets that contain information about many aspects different countries including economical, educational, health and lifestyle related, and the measured happiness of the people living in those countries. We collect tweets from people from said countries, and using sentiment analysis, which is a common NLP (Natural language processing) task, confirm the findings of the previous project milestone by comparing the average sentiment of tweets from a country, with the calculated happiness score from the dataset.
We collected tweets from two countries, Egypt and Sweden, for 4 days, the average sentiment of Egypt was about 0.3, and that of Sweden was 0.39. So a country like Sweden that has a higher happiness score than Egypt also has a higher average sentiment of tweets.
