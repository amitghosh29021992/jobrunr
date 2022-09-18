# jobrunr
JobRunr is an open source project that helps in setting up a centralized orchestrator for scheduling jobs. 
More details can be found in the below link.

https://www.jobrunr.io/en/documentation/

This tools supports a variety of databases like SQL, Mongo, in-memory databases etc. However it doesn't have support for Couchbase database.

I have created a custom Couchbase storage provider that can be plugged in with this tool to add Couchbase support.

Steps to use this library:

Include this java file in your project and write the below piece of code in your configuration class

<img width="1180" alt="image" src="https://user-images.githubusercontent.com/47706629/190920600-967d46d2-5cb9-4e07-8447-10d5af50f1f4.png">

Note: Pass in the revelant variable values related to your couchbase cluster.
