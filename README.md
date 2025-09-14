

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
   "~id","~label","name:String","year:String","director:String"
   "3ed24801-a100-476a-b403-f96e99018a63","movie","The Shawshank Redemption","1994","Frank Darabont"
   "2e77808c-0dfb-4281-8925-685c950b8ef2","movie","The Godfather","1972","Francis Ford Coppola"
   "d8e553ba-78c4-456f-9319-077144f59841","movie","The Dark Knight","2008","Christopher Nolan"
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
   edges--actor-movie.csv
   ```
   "~id","~from","~to","~label"
   "d9bfdfb4-9b07-489c-b653-70c1bf68b29c","c9e81239-34d0-4003-bca2-77677b4cd164","3ed24801-a100-476a-b403-f96e99018a63","acted_in"
   "a38ffff9-ac97-4a9d-a5ee-1768094effbd","ea8d8c2d-3d77-4447-95e9-67e40477bca2","2e77808c-0dfb-4281-8925-685c950b8ef2","acted_in"
   "cd3342ad-2277-4917-bc94-f71ca5c06ad8","6477bff4-f799-431e-9e5a-8da402d8947a","d8e553ba-78c4-456f-9319-077144f59841","acted_in"
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
   +--------------------+---------+
   | object             |   count |
   |--------------------+---------|
   | vertex (movie)     |    1000 |
   | vertex (actor)     |    2709 |
   | edge (actor-movie) |    4000 |
   +--------------------+---------+
   ```
   
5. run some queries on our data
   ```sh
   python main.py analyze   
   ```
   
   ```   
   (1) sample vertices (movies)
   
   +---------------------------------------------------+--------+----------------------+
   | name                                              |   year | director             |
   |---------------------------------------------------+--------+----------------------|
   | The Shawshank Redemption                          |   1994 | Frank Darabont       |
   | The Godfather                                     |   1972 | Francis Ford Coppola |
   | The Dark Knight                                   |   2008 | Christopher Nolan    |
   | The Godfather: Part II                            |   1974 | Francis Ford Coppola |
   | 12 Angry Men                                      |   1957 | Sidney Lumet         |
   | The Lord of the Rings: The Return of the King     |   2003 | Peter Jackson        |
   | Pulp Fiction                                      |   1994 | Quentin Tarantino    |
   | Schindler's List                                  |   1993 | Steven Spielberg     |
   | Inception                                         |   2010 | Christopher Nolan    |
   | Fight Club                                        |   1999 | David Fincher        |
   | The Lord of the Rings: The Fellowship of the Ring |   2001 | Peter Jackson        |
   | Forrest Gump                                      |   1994 | Robert Zemeckis      |
   | Il buono, il brutto, il cattivo                   |   1966 | Sergio Leone         |
   | The Lord of the Rings: The Two Towers             |   2002 | Peter Jackson        |
   | The Matrix                                        |   1999 | Lana Wachowski       |
   | Goodfellas                                        |   1990 | Martin Scorsese      |
   | Star Wars: Episode V - The Empire Strikes Back    |   1980 | Irvin Kershner       |
   | One Flew Over the Cuckoo's Nest                   |   1975 | Milos Forman         |
   | Hamilton                                          |   2020 | Thomas Kail          |
   | Gisaengchung                                      |   2019 | Bong Joon Ho         |
   +---------------------------------------------------+--------+----------------------+
   
   (2) sample vertices (actors)
   
   +---------------------+
   | name                |
   |---------------------|
   | Tim Robbins         |
   | Marlon Brando       |
   | Christian Bale      |
   | Al Pacino           |
   | Henry Fonda         |
   | Elijah Wood         |
   | John Travolta       |
   | Liam Neeson         |
   | Leonardo DiCaprio   |
   | Brad Pitt           |
   | Tom Hanks           |
   | Clint Eastwood      |
   | Lilly Wachowski     |
   | Robert De Niro      |
   | Mark Hamill         |
   | Jack Nicholson      |
   | Lin-Manuel Miranda  |
   | Kang-ho Song        |
   | Suriya              |
   | Matthew McConaughey |
   +---------------------+
   
   (3) top 10 actors who have starred with the most other actors
   
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
   
   (4) top 10 actors who have starred with both de niro and pacino
   
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
   
   (5) top 10 actors who have starred with both de niro and pacino, and their movies
   
   +----------------+------------------------+------------------------+
   | shared_actor   | movies (de niro)       | movies (pacino)        |
   |----------------+------------------------+------------------------|
   | Val Kilmer     | Heat                   | Heat                   |
   | Jon Voight     | Heat                   | Heat                   |
   | Harvey Keitel  | The Irishman           | The Irishman           |
   | Joe Pesci      | Goodfellas             | The Irishman           |
   | Diane Keaton   | The Godfather: Part II | The Godfather          |
   | John Cazale    | The Deer Hunter        | Dog Day Afternoon      |
   | Robert Duvall  | The Godfather: Part II | The Godfather: Part II |
   +----------------+------------------------+------------------------+ 
   ```



