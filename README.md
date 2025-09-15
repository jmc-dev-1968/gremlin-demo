

<!-- ABOUT THE PROJECT -->
## About The Project

This git project was created as a way to understand and demonstrate the use of common Neptune tasks such as   

1. transforming relational data (stored as CSV, residing in S3) to "graph-csv" format used by the Neptune Loader
2. loading this data into Neptune via the http endpoint https://neptune-endpoint:8182/loader using boto3 client.start_loader_job() method
3. querying the Neptune data via the web socket wss://neptune-endpoint:8182/gremlin using the Python gremlinpython lib

To facilitate local development (using PyCharm), I opted to employ a bastion server (EC2 instance running within Neptune's VPC) to open an SSH tunnel. The tunnel was created by using PuTTy.

This proved challening to say the least (a good amount of my code was creating Neptune utility classes to help  with the HTTP/WS communications). A good exercise in Neptune mechanics in any case.


<!-- GETTING STARTED -->
## Getting Started

The following AWS services need set up

1. Neptune 
2. EC2
3. S3 Bucket
4. IAM Role
5. IAM User / Keys
6. VPC Endpoint

Details have been omitted here but you can see these components referenced in the code. For this demo I am trying to focus on the Neptune data manipulation tasks. Typically I would create a companion project named **gremlin-demo-infra** containing all the AWS deployment scripts, IAM policies etc. 

### Prerequisites

* Sample data - from https://www.kaggle.com/datasets/harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows This is movie data from imdb.com (1000 movies). We only need the following columns :
  ```sh
  Series_Title, Released_Year, Director, Star1, Star2, Star3, Star4
  ```

### Usage

1. first we need to process the CSV data, transforming  to graph-csv format
   ```sh
   python main.py process
   ``` 
   the data will be saved locally (so the data can be perused in this project) as well as to S3, where it will be available for Neptune loading
 
   vertices--movie.csv
   ```
   "~id","~label","name:String","year:String"
   "0e412ecc-5fba-47cf-a870-c1a548b01e24","movie","The Shawshank Redemption","1994"
   "6b87f694-588d-472c-86c3-8370663abcb4","movie","The Godfather","1972"
   "4fc517aa-ab66-4e66-a40c-e100d3151632","movie","The Dark Knight","2008"
   ...
   ```
   vertices--actor.csv
   ```
   "~id","~label","name:String"
   "c9e81239-34d0-4003-bca2-77677b4cd164","actor","Tim Robbins"
   "ea8d8c2d-3d77-4447-95e9-67e40477bca2","actor","Marlon Brando"
   "6477bff4-f799-431e-9e5a-8da402d8947a","actor","Christian Bale"
   ...
   ```
   vertices--director.csv
   ```
   "~id","~label","name:String"
   "a0b0adaa-9b13-4dbf-a0e3-0530bb905b50","director","Frank Darabont"
   "46cdb583-f906-42a5-a71a-df637c74b291","director","Francis Ford Coppola"
   "04e9f15d-a14c-4b7c-8320-abc7e5f66783","director","Christopher Nolan"
   ...
   ```
   edges--actor-movie.csv
   ```
   "~id","~from","~to","~label"
   "d9bfdfb4-9b07-489c-b653-70c1bf68b29c","c9e81239-34d0-4003-bca2-77677b4cd164","3ed24801-a100-476a-b403-f96e99018a63","acted_in"
   "a38ffff9-ac97-4a9d-a5ee-1768094effbd","ea8d8c2d-3d77-4447-95e9-67e40477bca2","2e77808c-0dfb-4281-8925-685c950b8ef2","acted_in"
   "cd3342ad-2277-4917-bc94-f71ca5c06ad8","6477bff4-f799-431e-9e5a-8da402d8947a","d8e553ba-78c4-456f-9319-077144f59841","acted_in"
   ...
   ```
   edges--movie-director.csv
   ```
   "~id","~from","~to","~label"
   "c007337e-a235-44cc-8973-20f9b0ad49dd","0e412ecc-5fba-47cf-a870-c1a548b01e24","a0b0adaa-9b13-4dbf-a0e3-0530bb905b50","directed_by"
   "fcf15fc8-3904-4e16-9d96-87a14aa7d2af","6b87f694-588d-472c-86c3-8370663abcb4","46cdb583-f906-42a5-a71a-df637c74b291","directed_by"
   "fda7e2cf-3cd7-4680-8672-3dac5f8bfdc9","4fc517aa-ab66-4e66-a40c-e100d3151632","04e9f15d-a14c-4b7c-8320-abc7e5f66783","directed_by"
   ...
   ```


2. load vertices to Neptune (vertices need loaded first, before edges) 
   ```sh
   python main.py load_vertices
   ```
3. load edges to Neptune
   ```js
   python main.py load_edges
   ```
4. query the Neptune server
   ```sh
   python main.py summarize   
   ```
   
   ```   
   +-----------------------+---------+
   | object                |   count |
   |-----------------------+---------|
   | vertex (movie)        |    1000 |
   | vertex (actor)        |    2709 |
   | vertex (director)     |     548 |
   | edge (actor-movie)    |    4000 |
   | edge (movie-director) |    1000 |
   +-----------------------+---------+

   ```
   
5. run some gremlin queries on our data
   ```sh
   python main.py analyze   
   ```
   
   ```   
   (1) sample vertices (movies)
   
   +-----------------------------------------------+--------+
   | name                                          |   year |
   |-----------------------------------------------+--------|
   | The Shawshank Redemption                      |   1994 |
   | The Godfather                                 |   1972 |
   | The Dark Knight                               |   2008 |
   | The Godfather: Part II                        |   1974 |
   | 12 Angry Men                                  |   1957 |
   | The Lord of the Rings: The Return of the King |   2003 |
   | Pulp Fiction                                  |   1994 |
   | Schindler's List                              |   1993 |
   | Inception                                     |   2010 |
   | Fight Club                                    |   1999 |
   +-----------------------------------------------+--------+
   
   (2) sample vertices (actors)
   
   +-------------------+
   | name              |
   |-------------------|
   | Tim Robbins       |
   | Marlon Brando     |
   | Christian Bale    |
   | Al Pacino         |
   | Henry Fonda       |
   | Elijah Wood       |
   | John Travolta     |
   | Liam Neeson       |
   | Leonardo DiCaprio |
   | Brad Pitt         |
   +-------------------+
   
   (3) sample vertices (directors)
   
   +----------------------+
   | name                 |
   |----------------------|
   | Frank Darabont       |
   | Francis Ford Coppola |
   | Christopher Nolan    |
   | Sidney Lumet         |
   | Peter Jackson        |
   | Quentin Tarantino    |
   | Steven Spielberg     |
   | David Fincher        |
   | Robert Zemeckis      |
   | Sergio Leone         |
   +----------------------+
   
   (4) top 10 actors who have starred with the most other actors
   
   +-------------------+----------------------+
   | actor             |   fellow_actor_count |
   |-------------------+----------------------|
   | Robert De Niro    |                   45 |
   | Tom Hanks         |                   38 |
   | Brad Pitt         |                   36 |
   | Al Pacino         |                   35 |
   | Clint Eastwood    |                   33 |
   | Leonardo DiCaprio |                   32 |
   | Christian Bale    |                   31 |
   | Matt Damon        |                   31 |
   | James Stewart     |                   30 |
   | Johnny Depp       |                   27 |
   +-------------------+----------------------+
   
   (5) top 5 directors with the most movies
   
   +------------------+---------------+
   | director         |   movie_count |
   |------------------+---------------|
   | Alfred Hitchcock |            14 |
   | Steven Spielberg |            13 |
   | Hayao Miyazaki   |            11 |
   | Martin Scorsese  |            10 |
   | Akira Kurosawa   |            10 |
   +------------------+---------------+
   
   (6) top 10 actors who have starred with both de niro and pacino
   
   +----------------+
   | shared_actor   |
   |----------------|
   | Robert Duvall  |
   | Diane Keaton   |
   | Joe Pesci      |
   | Val Kilmer     |
   | Jon Voight     |
   | John Cazale    |
   | Harvey Keitel  |
   +----------------+
   
   (7) top 10 actors who have starred with both de niro and pacino, and their movies
   
   +----------------+------------------------+------------------------+
   | shared_actor   | movies (de niro)       | movies (pacino)        |
   |----------------+------------------------+------------------------|
   | Robert Duvall  | The Godfather: Part II | The Godfather: Part II |
   | Val Kilmer     | Heat                   | Heat                   |
   | Harvey Keitel  | The Irishman           | The Irishman           |
   | Jon Voight     | Heat                   | Heat                   |
   | Joe Pesci      | Goodfellas             | The Irishman           |
   | John Cazale    | The Deer Hunter        | Dog Day Afternoon      |
   | Diane Keaton   | The Godfather: Part II | The Godfather          |
   +----------------+------------------------+------------------------+

   ```



