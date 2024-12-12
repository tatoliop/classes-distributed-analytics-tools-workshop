## Pipeline

### Pull images from both compose files

`docker-compose pull`


### Run the kafka producer

`python producer.py`

### Build the job

Run the command `mvn clean package` in the wordcount folder or use the pre-built jar in `wordcount/target/wordcount-01.jar`.

### Run the job

1. Open the Flink UI using the url `localhost:8081`.
2. Go to `Submit new job`.
3. Add the jar file
4. Click on the job and start.

### Monitoring the word count

The word count job outputs the counters in the console of the taskmanager. Use the command `docker logs -f XXX` where XXX is the taskmanager container to monitor the logs.