# Recommender

"Recommender" is an attempt to provide recommendations of stories for users.

It learns by observing the read history of all users to recommend new stories. It uses the ALS Recommendation Algorithm from Spark-MlLib.
* `Learner` to train model based on the set of Read history of the user
* `Recommender` that recommends more stories based on the current story being read.
* `Generator` component is just generating training data(userReadHistory) based on user-interests. 


## API 

Recommender exposes an API to avail the recommendations

    /recommend?profileId=<profileId>&storyId=<storyId>

and the response is the Recommendation List where each Recommendation is a <storyId, confidence>

## TODO   
[ ] Support for profileIds and storyIds as String       
[ ] External storage for training data and model   
[ ] Running Spark in cluster
